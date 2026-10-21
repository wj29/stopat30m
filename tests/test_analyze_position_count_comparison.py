"""Synthetic sealed reports test plotting; no real models or ledger replay."""
from copy import deepcopy
from decimal import Decimal
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest


SPEC=importlib.util.spec_from_file_location("a10_plot_script",Path(__file__).parents[1]/"scripts/analyze_position_count_comparison.py")
module=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    module.write(path,value)


def reseal(source):
    save(source/"manifest.json",{"kind":module.KIND,"files":{str(p.relative_to(source)):module.sha(p)
        for p in sorted(source.rglob("*")) if p.is_file() and p!=source/"manifest.json"}})


def create_report(tmp_path):
    source=tmp_path/"sealed_synthetic"
    results=[]
    for index,n in enumerate(module.COUNTS):
        for period in module.PERIODS:
            directory=source/f"top{n}"/period;directory.mkdir(parents=True)
            dates={"2025":["2025-01-02","2025-01-03"],"2026":["2026-01-05","2026-01-06"],
                "continuous":["2025-01-02","2025-01-03"]}[period]
            valid=not(n==20 and period in {"2026","continuous"})
            lower=[Decimal(95000-index*1000),Decimal(105000-index*1000)]
            upper=[lower[0]+2000,lower[1]+5000]
            curve=pd.DataFrame([{"date":d,"equity_cny":str(lo),"equity_upper_bound_cny":str(hi),
                "nav":float(lo/100000),"accounting_valid":valid or k==0,"valuation_complete":True}
                for k,(d,lo,hi) in enumerate(zip(dates,lower,upper))])
            curve.to_csv(directory/"curves.csv",index=False)
            trade={"notional":"5005.00","commission":"5.00","transfer_fee":".05","stamp_tax":"0.00",
                "total_fee":"5.05","slippage_cost":"5.00"}
            pd.DataFrame([trade]).to_csv(directory/"trades.csv",index=False)
            peak=Decimal(100000);dd=[]
            for lo,hi in zip(lower,upper):peak=max(peak,hi);dd.append(lo/peak-1)
            metrics={"session_count":2,"final_equity_cny":str(lower[-1]),"total_return_lower_exact":str(lower[-1]/100000-1),
                "max_drawdown_conservative_exact":str(min([Decimal(0),*dd])),"accounting_invalid_days":int(not valid),
                "terminal_prices_fresh":valid,"declared_end_reached":True,"trade_count":1,
                "one_way_turnover_average_nav":.04,"total_distribution_tax_reserved_cny":"2.00",
                "cash_distribution_rounding_reserved_cny":".01",**{key+"_cny":value for key,value in trade.items()}}
            complete=valid and not(n==5 and period=="2025")
            daily=[{"date":day,"coverage_complete":complete,"max_single_nav_fraction":.9/n if complete else None,
                "max_top3_nav_fraction":min(.9,2.7/n) if complete else None,"effective_stock_count":n if complete else None,
                "risky_nav_fraction":.9 if complete else None} for day in dates]
            risk={"coverage_complete":complete,"max_single_nav_fraction":.9/n if complete else None,
                "max_top3_nav_fraction":min(.9,2.7/n) if complete else None,"min_effective_stock_count":n if complete else None,
                "mean_risky_nav_fraction":.9 if complete else None}
            diag={"kind":"a10_original_ledger_position_count_diagnostics","schema_version":1,"performance_valid":valid,
                "costs":{"money_reconciliation_passed":True,"cost_addback_performed":False,"fill_count":1,
                         **{key+"_cny":value for key,value in trade.items()}},"risk":risk,"daily_exposures":daily}
            row={"target_count":n,"period":period,"model":"LGB","variant":{"key":"hold10_cap2","minimum_hold_sessions":10,"max_replacements":2},
                "performance_valid":valid,"metrics_basis":"declared_accounting_NAV_not_actual_liquidation" if valid else "invalid_indicative_carried_ledger_do_not_use_for_selection",
                "metrics":metrics,"stopped":None,"actual_last_valued_session":dates[-1],
                "diagnostics":{k:v for k,v in diag.items() if k!="daily_exposures"}}
            save(directory/"summary.json",row);save(directory/"diagnostics.json",diag);results.append(row)
    report={"kind":module.KIND,"native_fits":0,"predictions_regenerated":False,"active_paper_changed_by_experiment":False,
        "live_broker_orders":0,"results":results,"all_performance_valid":False,
        "baseline_full_reproduction":{p:{"passed":True} for p in module.PERIODS},
        "decision":{"status":"INCONCLUSIVE_INVALID_REFERENCE","current_paper_action":"keep_existing_top20_unchanged","strategy_promoted":False},
        "synthetic_test_fixture_not_real_research":True}
    save(source/"report.json",report)
    save(source/"status.json",{"phase":"completed","all_performance_valid":False})
    save(source/"declaration.json",{"design":{"initial_cash_cny":"100000.00","target_counts":[20,15,10,5]},"synthetic":True})
    code=source/"source"/"synthetic_runtime.py";code.parent.mkdir();code.write_bytes(b"# Explicit synthetic frozen source fixture\n")
    reseal(source)
    return source


@pytest.fixture
def source(tmp_path):
    return create_report(tmp_path)


def test_initial_cash_peak_and_all_terminal_days_and_unknown_risk_are_preserved(source):
    _,_,_,report,paths,initial=module.load_report(source/"report.json")
    assert initial==100000 and len(paths)==12
    path=paths[20,"2025"]
    assert path["drawdown"][0]==-.05  # cash100000, not first observed upper97000.
    assert path["nav"][-1]==1.05 and path["dates"][-1]=="2025-01-03"
    assert paths[20,"2026"]["valid"] is False
    assert len(paths[20,"2026"]["dates"])==2
    assert all(v is None for v in paths[5,"2025"]["risk_series"]["max_single_nav_fraction"])
    assert report["decision"]["strategy_promoted"] is False


def test_synthetic_render_freezes_inputs_all_twelve_paths_and_keeps_costs_out_of_nav(source,tmp_path):
    before={str(p.relative_to(source)):module.sha(p) for p in source.rglob("*") if p.is_file()}
    output=tmp_path/"plots"
    result=module.analyze(source/"report.json",output)
    assert result["status"]=="completed" and result["original_curve_rows"]==24
    assert len(result["exact_paths"])==12
    assert result["invalid_paths_preserved"]==["top20/2026","top20/continuous"]
    assert result["unknown_risk_sessions"]["top5/2025"]==2
    assert result["unknown_concentration_imputed_zero"] is False
    for stem in ("position_count_nav_drawdown","position_count_return_dd_turnover_cost","position_count_concentration"):
        assert (output/(stem+".png")).stat().st_size>1000
        assert (output/(stem+".svg")).stat().st_size>1000
    inputs=module.read(output/"plot-input.json")
    assert inputs["costs_added_back_to_nav"] is False and len(inputs["paths"])==12
    path=next(p for p in inputs["paths"] if p["target_count"]==20 and p["period"]=="2025")
    assert path["nav"]==[.95,1.05] and path["drawdown"][0]==-.05
    assert path["cash_start_anchor"]["is_market_observation"] is False
    assert path["costs"]["commission_cny"]=="5.00"
    for name in ("report.json","manifest.json","declaration.json","status.json"):
        assert (output/("input_"+name)).read_bytes()==(source/name).read_bytes()
    assert (output/"input_source/synthetic_runtime.py").read_bytes()==(source/"source/synthetic_runtime.py").read_bytes()
    assert before=={str(p.relative_to(source)):module.sha(p) for p in source.rglob("*") if p.is_file()}
    assert result["plot_implementation_sha256"]==module.sha(output/"plot_source.py")
    assert all(module.sha(output/name)==digest for name,digest in result["files"].items())
    svg=(output/"position_count_nav_drawdown.svg").read_text()
    assert "indicative INVALID" in svg and "stroke-dasharray" in svg
    assert "UNKNOWN days" in (output/"position_count_concentration.svg").read_text()
    assert "<pattern " in (output/"position_count_return_dd_turnover_cost.svg").read_text()


def test_changed_source_bytes_fail_before_output_is_created(source,tmp_path):
    (source/"top5/2025/curves.csv").write_text("changed")
    output=tmp_path/"plots"
    with pytest.raises(ValueError,match="artifact"):
        module.analyze(source/"report.json",output)
    assert not output.exists()


@pytest.mark.parametrize("fault",["missing_path","duplicate_path","unexpected_summary","incomplete_status","validity_flip","unknown_as_zero","initial_peak_omitted"])
def test_incomplete_or_semantically_changed_artifacts_are_rejected_even_with_new_checksums(source,fault):
    report=module.read(source/"report.json")
    if fault=="missing_path":report["results"].pop()
    elif fault=="duplicate_path":report["results"][-1]=deepcopy(report["results"][0])
    elif fault=="unexpected_summary":save(source/"top1/2025/summary.json",{"unexpected":True})
    elif fault=="incomplete_status":save(source/"status.json",{"phase":"ledger"})
    else:
        row=next(r for r in report["results"] if r["target_count"]==20 and r["period"]=="2026")
        directory=source/"top20/2026"
        if fault=="validity_flip":row["performance_valid"]=True
        elif fault=="unknown_as_zero":
            diag=module.read(directory/"diagnostics.json");diag["risk"]["max_single_nav_fraction"]=0
            row["diagnostics"]={k:v for k,v in diag.items() if k!="daily_exposures"};save(directory/"diagnostics.json",diag)
        else:
            row["metrics"]["max_drawdown_conservative_exact"]=str(Decimal(95000)/97000-1)
        save(directory/"summary.json",row)
    save(source/"report.json",report);reseal(source)
    with pytest.raises(ValueError):module.load_report(source/"report.json")


def test_output_cannot_modify_the_source_or_overwrite_a_previous_figure_run(source,tmp_path):
    with pytest.raises(ValueError,match="new output"):
        module.analyze(source/"report.json",source/"plots")
    output=tmp_path/"existing";output.mkdir()
    with pytest.raises(ValueError,match="new output"):
        module.analyze(source/"report.json",output)
