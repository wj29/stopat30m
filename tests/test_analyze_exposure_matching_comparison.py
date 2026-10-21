"""Synthetic sealed reports test plotting; no real models or ledger replay."""
from copy import deepcopy
from decimal import Decimal
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest


SPEC=importlib.util.spec_from_file_location("a12_plot_script",Path(__file__).parents[1]/"scripts/analyze_exposure_matching_comparison.py")
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
    results=[];diagnostics={}
    for index,n in enumerate(module.ARMS):
        for period in module.PERIODS:
            directory=source/n/period;directory.mkdir(parents=True)
            dates={"2025":["2025-01-02","2025-01-03"],"2026":["2026-01-05","2026-01-06"],
                "continuous":["2025-01-02","2025-01-03"]}[period]
            valid=not(n=="hold10_cap2" and period in {"2026","continuous"})
            lower=[Decimal(95000-index*1000),Decimal(105000-index*1000)]
            upper=[lower[0]+2000,lower[1]+5000]
            curve=pd.DataFrame([{"date":d,"equity_cny":str(lo),"equity_upper_bound_cny":str(hi),
                "nav":float(lo/100000),"accounting_valid":valid or k==0,"valuation_complete":True}
                for k,(d,lo,hi) in enumerate(zip(dates,lower,upper))])
            curve.to_csv(directory/"curves.csv",index=False)
            trade={"notional":"5005.00","commission":"5.00","transfer_fee":".05","stamp_tax":"0.00",
                "total_fee":"5.05","slippage_cost":"5.00"}
            trades=[trade]
            if n=="matched_original":
                trades=[dict(trade,order_purpose="core"),dict(trade,order_purpose="cash_control")]
            pd.DataFrame(trades).to_csv(directory/"trades.csv",index=False)
            sums={key+"_cny":str(sum((Decimal(row[key]) for row in trades),Decimal(0))) for key in trade}
            peak=Decimal(100000);dd=[]
            for lo,hi in zip(lower,upper):peak=max(peak,hi);dd.append(lo/peak-1)
            metrics={"session_count":2,"final_equity_cny":str(lower[-1]),"total_return_lower_exact":str(lower[-1]/100000-1),
                "max_drawdown_conservative_exact":str(min([Decimal(0),*dd])),"accounting_invalid_days":int(not valid),
                "terminal_prices_fresh":valid,"declared_end_reached":True,"trade_count":len(trades),
                "one_way_turnover_average_nav":.04,"total_distribution_tax_reserved_cny":"2.00",
                "cash_distribution_rounding_reserved_cny":".01",**sums}
            complete=valid and not(n=="matched_original" and period=="2025")
            daily=[{"date":day,"coverage_complete":complete,"max_single_nav_fraction":.9/20 if complete else None,
                "max_top3_nav_fraction":min(.9,2.7/20) if complete else None,"effective_stock_count":20 if complete else None,
                "risky_nav_fraction":.9 if complete else None} for day in dates]
            risk={"coverage_complete":complete,"max_single_nav_fraction":.9/20 if complete else None,
                "max_top3_nav_fraction":min(.9,2.7/20) if complete else None,"min_effective_stock_count":20 if complete else None,
                "mean_risky_nav_fraction":.9 if complete else None}
            diag={"kind":"a10_original_ledger_position_count_diagnostics","schema_version":1,"performance_valid":valid,
                "costs":{"money_reconciliation_passed":True,"cost_addback_performed":False,"fill_count":len(trades),
                         **sums},"risk":risk,"daily_exposures":daily}
            buckets={purpose:{"fill_count":0,**{key+"_cny":Decimal(0) for key in trade}} for purpose in ("core","cash_control")}
            for fill in trades:
                purpose=fill.get("order_purpose","core");buckets[purpose]["fill_count"]+=1
                for key in trade:buckets[purpose][key+"_cny"]+=Decimal(fill[key])
            buckets={purpose:{k:str(v) if isinstance(v,Decimal) else v for k,v in values.items()} for purpose,values in buckets.items()}
            row={"arm_key":n,"period":period,"model":"LGB","variant":{"key":"hold10_cap2","minimum_hold_sessions":10,"max_replacements":2},
                "performance_valid":valid,"metrics_basis":"declared_accounting_NAV_not_actual_liquidation" if valid else "invalid_indicative_carried_ledger_do_not_use_for_selection",
                "metrics":metrics,"stopped":None,"actual_last_valued_session":dates[-1],
                "purpose_costs":buckets,"diagnostics":{k:v for k,v in diag.items() if k!="daily_exposures"}}
            save(directory/"summary.json",row);save(directory/"diagnostics.json",diag);results.append(row)
            diagnostics[n,period]=diag
    from stopat30m.ashare_exposure_matching_experiment import exposure_match_diagnostics
    matching={}
    for period in module.PERIODS:
        matching[period]=exposure_match_diagnostics(diagnostics["cap7p5pct",period],diagnostics["matched_original",period],
            reference_valid=True,matched_valid=True)
        save(source/"matched_original"/period/"matching.json",matching[period])
    report={"kind":module.KIND,"native_fits":0,"predictions_regenerated":False,"active_paper_changed_by_experiment":False,
        "live_broker_orders":0,"results":results,"all_performance_valid":False,
        "baseline_full_reproduction":{a+"/"+p:{"passed":True} for a in module.ARMS[:2] for p in module.PERIODS},
        "matching_by_period":matching,
        "decision":{"status":"INCONCLUSIVE_INVALID_REFERENCE","current_paper_action":"keep_existing_top20_unchanged","strategy_promoted":False},
        "synthetic_test_fixture_not_real_research":True}
    save(source/"report.json",report)
    save(source/"status.json",{"phase":"completed","all_performance_valid":False})
    save(source/"declaration.json",{"design":json.loads((Path(__file__).parents[1]/"docs/research/a12_exposure_matching_design_v1.json").read_bytes()),"synthetic":True})
    code=source/"source"/"synthetic_runtime.py";code.parent.mkdir();code.write_bytes(b"# Explicit synthetic frozen source fixture\n")
    reseal(source)
    return source


@pytest.fixture
def source(tmp_path):
    return create_report(tmp_path)


def test_initial_cash_peak_and_all_terminal_days_and_unknown_risk_are_preserved(source):
    _,_,_,report,paths,initial=module.load_report(source/"report.json")
    assert initial==100000 and len(paths)==9
    path=paths["hold10_cap2","2025"]
    assert path["drawdown"][0]==-.05  # cash100000, not first observed upper97000.
    assert path["nav"][-1]==1.05 and path["dates"][-1]=="2025-01-03"
    assert paths["hold10_cap2","2026"]["valid"] is False
    assert len(paths["hold10_cap2","2026"]["dates"])==2
    assert all(v is None for v in paths["matched_original","2025"]["risk_series"]["max_single_nav_fraction"])
    assert report["decision"]["strategy_promoted"] is False


def test_synthetic_render_freezes_inputs_all_nine_paths_and_keeps_costs_out_of_nav(source,tmp_path):
    before={str(p.relative_to(source)):module.sha(p) for p in source.rglob("*") if p.is_file()}
    output=tmp_path/"plots"
    result=module.analyze(source/"report.json",output)
    assert result["status"]=="completed" and result["original_curve_rows"]==18
    assert len(result["exact_paths"])==9
    assert result["invalid_paths_preserved"]==["hold10_cap2/2026","hold10_cap2/continuous"]
    assert result["unknown_risk_sessions"]["matched_original/2025"]==2
    assert result["unknown_concentration_imputed_zero"] is False
    for stem in ("exposure_matching_nav_drawdown","exposure_matching_return_dd_turnover_cost","exposure_matching_exposure_gaps"):
        assert (output/(stem+".png")).stat().st_size>1000
        assert (output/(stem+".svg")).stat().st_size>1000
    inputs=module.read(output/"plot-input.json")
    assert inputs["costs_added_back_to_nav"] is False and len(inputs["paths"])==9
    path=next(p for p in inputs["paths"] if p["arm_key"]=="hold10_cap2" and p["period"]=="2025")
    assert path["nav"]==[.95,1.05] and path["drawdown"][0]==-.05
    assert path["cash_start_anchor"]["is_market_observation"] is False
    assert path["costs"]["core_fee_cny"]=="5.05" and path["costs"]["cash_control_fee_cny"]=="0"
    for name in ("report.json","manifest.json","declaration.json","status.json"):
        assert (output/("input_"+name)).read_bytes()==(source/name).read_bytes()
    assert (output/"input_source/synthetic_runtime.py").read_bytes()==(source/"source/synthetic_runtime.py").read_bytes()
    assert before=={str(p.relative_to(source)):module.sha(p) for p in source.rglob("*") if p.is_file()}
    assert result["plot_implementation_sha256"]==module.sha(output/"plot_source.py")
    assert all(module.sha(output/name)==digest for name,digest in result["files"].items())
    svg=(output/"exposure_matching_nav_drawdown.svg").read_text()
    assert "indicative INVALID" in svg and "stroke-dasharray" in svg
    assert "UNKNOWN paired days" in (output/"exposure_matching_exposure_gaps.svg").read_text()
    assert "<pattern " in (output/"exposure_matching_return_dd_turnover_cost.svg").read_text()


def test_changed_source_bytes_fail_before_output_is_created(source,tmp_path):
    (source/"matched_original/2025/curves.csv").write_text("changed")
    output=tmp_path/"plots"
    with pytest.raises(ValueError,match="artifact"):
        module.analyze(source/"report.json",output)
    assert not output.exists()


@pytest.mark.parametrize("fault",["missing_path","duplicate_path","unexpected_summary","incomplete_status","validity_flip","unknown_as_zero","initial_peak_omitted"])
def test_incomplete_or_semantically_changed_artifacts_are_rejected_even_with_new_checksums(source,fault):
    report=module.read(source/"report.json")
    if fault=="missing_path":report["results"].pop()
    elif fault=="duplicate_path":report["results"][-1]=deepcopy(report["results"][0])
    elif fault=="unexpected_summary":save(source/"undeclared_cap/2025/summary.json",{"unexpected":True})
    elif fault=="incomplete_status":save(source/"status.json",{"phase":"ledger"})
    else:
        row=next(r for r in report["results"] if r["arm_key"]=="hold10_cap2" and r["period"]=="2026")
        directory=source/"hold10_cap2/2026"
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


@pytest.mark.parametrize("fault",["missing_control_guard","changed_design","wrong_matching_status","missing_unknown_date",
    "wrong_daily_gap","duplicate_purpose_cost","missing_purpose"])
def test_control_matching_qualification_and_actual_cost_buckets_cannot_be_forged(source,fault):
    report=module.read(source/"report.json")
    if fault=="missing_control_guard":report["baseline_full_reproduction"].pop("cap7p5pct/2025")
    elif fault=="changed_design":
        declaration=module.read(source/"declaration.json");declaration["design"]["cash_controller"]["target_for_session_t"]="same_day_or_full_mean"
        save(source/"declaration.json",declaration)
    elif fault in {"wrong_matching_status","missing_unknown_date","wrong_daily_gap"}:
        period="2025" if fault!="wrong_daily_gap" else "2026"
        stat=report["matching_by_period"][period]
        if fault=="wrong_matching_status":stat.update(status="MATCHED",passed=True)
        elif fault=="missing_unknown_date":stat["unknown_dates"].pop()
        else:stat["daily_gaps"][0]["signed_gap_exact"]=".02"
        save(source/"matched_original"/period/"matching.json",stat)
    elif fault=="duplicate_purpose_cost":
        row=next(r for r in report["results"] if r["arm_key"]=="matched_original" and r["period"]=="2026")
        row["purpose_costs"]["core"]["total_fee_cny"]="10.10"
        save(source/"matched_original/2026/summary.json",row)
    else:
        p=source/"matched_original/2026/trades.csv";frame=pd.read_csv(p);frame.drop(columns="order_purpose").to_csv(p,index=False)
    save(source/"report.json",report);reseal(source)
    with pytest.raises(ValueError):module.load_report(source/"report.json")


def test_matched_overlay_costs_are_separate_and_count_main_fills_once(source):
    _,_,_,_,paths,_=module.load_report(source/"report.json")
    cost=paths["matched_original","2026"]["costs"]
    assert cost["core_fee_cny"]==cost["cash_control_fee_cny"]=="5.05"
    assert cost["core_slippage_cny"]==cost["cash_control_slippage_cny"]=="5.00"
    assert sum(Decimal(cost[k]) for k,_,_ in module.COSTS)==Decimal("22.11")
    assert "total_fee_cny" not in cost  # not added on top of the two fee buckets.


def test_stopped_matched_prefix_is_preserved_without_creating_terminal_nav_rows(source):
    from stopat30m.ashare_exposure_matching_experiment import exposure_match_diagnostics
    report=module.read(source/"report.json");directory=source/"matched_original/2026"
    row=next(r for r in report["results"] if r["arm_key"]=="matched_original" and r["period"]=="2026")
    frame=pd.read_csv(directory/"curves.csv").iloc[:1].copy();frame.to_csv(directory/"curves.csv",index=False)
    row.update(performance_valid=False,metrics_basis="invalid_indicative_stopped_ledger",stopped={"kind":"unavailable_opening_nav"},actual_last_valued_session=frame.date.iloc[0])
    lower=Decimal(str(frame.equity_cny.iloc[0]));upper=Decimal(str(frame.equity_upper_bound_cny.iloc[0]))
    row["metrics"].update(session_count=1,final_equity_cny=str(lower),total_return_lower_exact=str(lower/100000-1),
        max_drawdown_conservative_exact=str(lower/max(Decimal(100000),upper)-1),terminal_prices_fresh=False,declared_end_reached=False)
    diag=module.read(directory/"diagnostics.json");diag["performance_valid"]=False;diag["daily_exposures"]=diag["daily_exposures"][:1]
    row["diagnostics"]={k:v for k,v in diag.items() if k!="daily_exposures"}
    save(directory/"summary.json",row);save(directory/"diagnostics.json",diag)
    stat=exposure_match_diagnostics(module.read(source/"cap7p5pct/2026/diagnostics.json"),diag,reference_valid=True,matched_valid=False)
    report["matching_by_period"]["2026"]=stat;save(directory/"matching.json",stat);save(source/"report.json",report);reseal(source)
    src,_,_,report,paths,_=module.load_report(source/"report.json")
    assert len(paths["matched_original","2026"]["dates"])==1
    matching=module.load_matching(src,report,paths)["2026"]
    assert matching["statistics"]["status"]=="INVALID_PATH" and matching["daily_signed_gap"]==[0,None]
    assert matching["matched_risky_nav_fraction"]==[.9,None]
