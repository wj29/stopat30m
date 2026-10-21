"""A12 declaration/matching/orchestration tests on synthetic inputs only."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json
import sys

import pytest
import pandas as pd

from stopat30m import ashare_exposure_matching_experiment as experiment
from test_ashare_position_count_experiment import _reference, _write

ARMS = ("hold10_cap2", "cap7p5pct", "matched_original")
PERIODS = ("2025", "2026", "continuous")


def _design():
    return json.loads((Path(__file__).parents[1]/"docs/research/a12_exposure_matching_design_v1.json").read_bytes())


def _metrics():
    return {"total_return_lower_exact": "0.10", "max_drawdown_conservative_exact": "-0.20",
        "total_fee_cny": "1000.00", "slippage_cost_cny": "100.00", "mean_invested_fraction": .8,
        "one_way_turnover_average_nav": 8.0, "positive_month_fraction": .5,
        "rolling_63_sessions": {"minimum": -.1}}


def _diagnostics(values, *, unknown=(), start="2025-01-02"):
    """Complete explicit synthetic date rows; unknown rows retain their gaps."""
    first = date.fromisoformat(start)
    days = [(first+timedelta(days=i)).isoformat() for i in range(len(values))]
    daily = [{"date": day, "coverage_complete": i not in unknown,
        "risky_nav_fraction": float(value) if i not in unknown else None,
        "coverage_gaps": [] if i not in unknown else ["synthetic_unbounded_claim"],
        "cash_nav_fraction": float(Decimal(1)-Decimal(str(value))),
        "nav_lower_cny": "100000.00", "cash_cny": str((Decimal(1)-Decimal(str(value)))*100000)}
        for i,(day,value) in enumerate(zip(days,values))]
    return {"kind": "a10_original_ledger_position_count_diagnostics", "daily_exposures": daily,
        "risk": {"coverage_complete": not unknown,
            "mean_risky_nav_fraction": sum(map(float,values))/len(values) if values and not unknown else None}}


def _match(reference, matched, *, reference_valid=True, matched_valid=True):
    return experiment.exposure_match_diagnostics(reference, matched,
        reference_valid=reference_valid, matched_valid=matched_valid)


def test_design_canonical_is_fixed_and_never_calibrates_a_full_period_mean():
    design = _design()
    experiment._check_design(design)
    assert design["arms"] == list(ARMS)
    assert design["cash_controller"]["target_for_session_t"].startswith("previous_actual_session_")
    assert "future_mean_target" in design["cash_controller"]["first_scheduled_batch"]
    assert design["budget"]["native_fits"] == design["budget"]["regenerated_predictions"] == 0
    assert design["automatic_promotion"] is design["retrain"] is False


@pytest.mark.parametrize("section,key,value", [
    (None, "arms", ["hold10_cap2", "cap7p5pct", "matched_original", "optimized"]),
    (None, "evaluation_end", "2026-10-16"), (None, "initial_cash_cny", "200000.00"),
    (None, "model", "refitted_H20"), (None, "automatic_promotion", True),
    (None, "repair_old_Cinda_accounting", True),
    ("cash_controller", "target_for_session_t", "full_period_fitted_mean"),
    ("cash_controller", "first_scheduled_batch", "use_future_first_close"),
    ("cash_controller", "no_trade_band_absolute_fraction", "0.02"),
    ("cash_controller", "target_missing_or_incomplete_or_out_of_range", "carry_or_zero"),
    ("cash_controller", "fees", "linear_fee_scaling"),
    ("matching_gate", "known_same_actual_close_security_exposure_coverage_min", .98),
    ("matching_gate", "abs_mean_signed_gap_max", ".02"),
    ("matching_gate", "mean_absolute_gap_max", ".03"),
    ("matching_gate", "p95_absolute_gap_max", ".06"),
    ("attribution", "interpretation", "causal_pure_turnover_alpha"),
    ("budget", "reported_ledgers", 12), ("budget", "native_fits", 1),
])
def test_no_post_result_design_budget_matching_or_attribution_changes(section,key,value):
    design = _design(); (design if section is None else design[section])[key] = value
    with pytest.raises(ValueError): experiment._check_design(design)


def _control_reference(folder, *, key, fraction, period):
    result = _reference(folder, period=period, valid=period != "continuous")
    result["metrics"].update(_metrics())
    for i,row in enumerate(result["records"]["trades"]):
        row.update(notional=str((i+1)*1000), transfer_fee=".01", stamp_tax="0.00",
            total_fee=str(Decimal(row["commission"])+Decimal(".01")), slippage_cost=".20")
    for field in ("notional", "commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost"):
        result["metrics"][field+"_cny"]=str(sum((Decimal(r[field]) for r in result["records"]["trades"]),Decimal(0)))
    pd.DataFrame(result["records"]["trades"]).to_csv(folder/"trades.csv",index=False)
    if fraction is not None:
        result["records"]["capital_turnover"] = [{"session": "2026-10-19", "fraction": fraction,
            "initial_build": False, "frozen_quantities": {"SH600000": 100}, "proofs_checked": True}]
        _write(folder/"capital_turnover.json", result["records"]["capital_turnover"])
    diagnostic = _diagnostics([.8], start="2026-10-19")
    _write(folder/"diagnostics.json", diagnostic)
    summary = {k: deepcopy(v) for k,v in result.items() if k not in {"records", "final_state"}}
    summary.update(arm_key=key, fraction=fraction,
        diagnostics={k:v for k,v in diagnostic.items() if k != "daily_exposures"})
    _write(folder/"summary.json", summary)
    return result, diagnostic


@pytest.fixture
def declaration_case(tmp_path, monkeypatch):
    source_files = {}
    for name in ("protocol", "market_audit", "merger_actions"):
        path=tmp_path/(name+".json");_write(path,{"synthetic":name});source_files[name]=experiment._proof(path)
    paper=tmp_path/"paper";active_code=tmp_path/"active-paper.py";active_code.write_text("# unchanged synthetic active paper\n")
    _write(paper/"manifest.json", {"kind":"isolated_ashare_low_turnover_forward", "strategy":{"top_k":20},
        "implementation_sha256":{str(active_code):experiment._proof(active_code)["sha256"]}})
    _write(paper/"state.json", {"cash":"14258.12", "positions":{"SH600000":{"quantity":100}}})
    source_files["active_paper_manifest"]=experiment._proof(paper/"manifest.json")
    source_files["active_code:"+str(active_code)]=experiment._proof(active_code)
    parent={"kind":"a11_fixed_lgb_capital_turnover_declaration", "bound_inputs":source_files,
        "source":{"snapshot":str(tmp_path/"snapshot"),"scope_directory":str(tmp_path/"scope"),
                  "predictions_directory":str(tmp_path/"models")},
        "reference_directory":str(tmp_path/"old-a6"),"active_paper":str(paper),
        "fits_performed":0,"predictions_regenerated":False}
    parent_path=tmp_path/"parent.json";_write(parent_path,parent)
    reference=tmp_path/"a11";_write(reference/"declaration.json",parent)
    references={};diagnostics={};rows=[]
    for key,fraction in (("hold10_cap2",None),("cap5pct","0.05"),("cap7p5pct","0.075"),("cap10pct","0.10")):
        for period in PERIODS:
            result,diag=_control_reference(reference/key/period,key=key,fraction=fraction,period=period)
            references[key,period]=result;diagnostics[key,period]=diag
            rows.append(json.loads((reference/key/period/"summary.json").read_bytes()))
    _write(reference/"report.json",{"kind":"a11_fixed_lgb_capital_turnover_results","results":rows,"all_performance_valid":False})
    _write(reference/"status.json",{"phase":"completed","all_performance_valid":False})
    _write(reference/"manifest.json",{"kind":"a11_fixed_lgb_capital_turnover_results","files":{
        str(p.relative_to(reference)):experiment._proof(p)["sha256"] for p in sorted(reference.rglob("*")) if p.is_file()}})
    design=tmp_path/"design.json";_write(design,_design())
    codes={}
    for name in ("a12_experiment","a12_validation","a12_execution"):
        p=tmp_path/(name+".py");p.write_text("# fixed synthetic "+name+"\n");codes[name]=p
    calls=[]
    monkeypatch.setattr(experiment,"implementation_proofs",lambda:{k:experiment._proof(p) for k,p in codes.items()})
    monkeypatch.setattr(experiment,"verify_reused_stability_models",lambda *args:calls.append(("verify",args)) or {"status":"passed","training_reperformed":False})
    monkeypatch.setattr(experiment,"load_stability_context",lambda *args:pytest.fail("No actual sources/simulator in synthetic tests"))
    return SimpleNamespace(arguments={"parent_declaration":parent_path,"reference_directory":reference,
        "design_path":design,"output":tmp_path/"declared.json"},parent=parent,parent_path=parent_path,
        reference=reference,paper=paper,active_code=active_code,codes=codes,calls=calls,tmp=tmp_path,
        references=references,diagnostics=diagnostics)


def _freeze(case):return experiment.freeze_exposure_matching_declaration(**case.arguments)


def test_freeze_inherits_sources_binds_original_a11_package_and_preserves_current_paper(declaration_case):
    c=declaration_case;before={p:p.read_bytes() for p in c.paper.iterdir()};sealed=_freeze(c)
    assert sealed["source"]==c.parent["source"] and sealed["active_paper"]==c.parent["active_paper"]
    assert sealed["reference_directory"]==str(c.reference)
    assert all(sealed["bound_inputs"][k]==p for k,p in c.parent["bound_inputs"].items())
    assert sealed["fits_performed"]==0 and sealed["predictions_regenerated"] is False
    assert sealed["active_state_is_observation_not_frozen_input"] is True
    assert all(p.read_bytes()==raw for p,raw in before.items())
    bound_paths={p["path"] for p in sealed["bound_inputs"].values()}
    assert str(c.reference/"manifest.json") in bound_paths
    assert str(c.reference/"hold10_cap2/2025/trades.csv") in bound_paths
    assert str(c.reference/"cap7p5pct/2026/capital_turnover.json") in bound_paths
    assert len(c.calls)==1


@pytest.mark.parametrize("changed",["code","parent_bound","reference_trade","reference_declaration"])
def test_freeze_source_or_control_tamper_is_blocked_without_context_or_paper_write(declaration_case,changed):
    c=declaration_case
    path=c.active_code if changed=="code" else Path(c.parent["bound_inputs"]["protocol"]["path"]) if changed=="parent_bound" else c.reference/"hold10_cap2/2025/trades.csv" if changed=="reference_trade" else c.reference/"declaration.json"
    path.write_text("changed original bytes")
    with pytest.raises(ValueError):_freeze(c)
    assert not c.arguments["output"].exists()


def test_freeze_and_runtime_never_overwrite_existing_outputs(declaration_case):
    c=declaration_case;_freeze(c);original=c.arguments["output"].read_bytes()
    with pytest.raises(ValueError):_freeze(c)
    assert c.arguments["output"].read_bytes()==original
    out=c.tmp/"existing";out.mkdir();(out/"keep").write_text("preserve")
    with pytest.raises(ValueError):experiment.run_exposure_matching_comparison(c.arguments["output"],out)
    assert (out/"keep").read_text()=="preserve" and len(c.calls)==1


def test_exact_1pp_serialized_decimal_gap_is_inclusive_not_float_subtraction():
    result=_match(_diagnostics([.8]*100),_diagnostics([.81]*100))
    assert result["status"]=="MATCHED" and result["passed"] is True
    assert result["mean_signed_gap"]==pytest.approx(.01)
    assert all(Decimal(r["signed_gap_exact"])==Decimal(".01") for r in result["daily_gaps"])


@pytest.mark.parametrize("unknown_count,passed",[(0,True),(1,True),(2,False)])
def test_99_percent_known_coverage_is_inclusive_and_unknown_not_zero(unknown_count,passed):
    ref=_diagnostics([.8]*100);matched=_diagnostics([.8]*100,unknown=range(unknown_count))
    before=deepcopy((ref,matched));r=_match(ref,matched)
    assert r["passed"] is passed and r["known_exposure_days"]==100-unknown_count
    assert r["known_exposure_coverage"]==(100-unknown_count)/100
    assert len(r["unknown_dates"])==unknown_count and len(r["daily_gaps"])==100-unknown_count
    assert r["mean_absolute_gap"]==0
    if not passed: assert r["status"]=="UNKNOWN_EXPOSURE_COVERAGE"
    assert (ref,matched)==before


@pytest.mark.parametrize("gate,gaps",[
    ("abs_mean_signed_gap_at_most_1pp",[.010001]*100),
    ("mean_absolute_gap_at_most_2pp",[.020001,-.020001]*50),
    ("p95_absolute_gap_at_most_5pp",[.050001,-.050001]*5+[0]*90),
])
def test_bias_absolute_error_and_tail_gate_fail_independently(gate,gaps):
    ref=_diagnostics([.5]*100)
    matched=_diagnostics([float(Decimal(".5")+Decimal(str(g))) for g in gaps])
    r=_match(ref,matched)
    assert r["status"]=="MATCHING_FAILED" and r["passed"] is False
    assert r["checks"][gate] is False
    assert all(value is True for key,value in r["checks"].items() if key!=gate)


def test_two_percent_MAE_and_five_percent_linear_percentile_boundaries_are_inclusive():
    gaps=[.02,-.02]*50
    r=_match(_diagnostics([.5]*100),_diagnostics([float(Decimal(".5")+Decimal(str(v))) for v in gaps]))
    assert r["passed"] is True and r["mean_absolute_gap"]==.02
    # 90 zeroes +10 exactly5pp gaps: interpolation at index94.05 stays exactly5pp.
    gaps=[.05,-.05]*5+[0]*90
    r=_match(_diagnostics([.5]*100),_diagnostics([float(Decimal(".5")+Decimal(str(v))) for v in gaps]))
    assert r["passed"] is True and r["p95_absolute_gap"]==.05
    assert r["above_5pp_dates"]==[] and r["longest_known_above_5pp_run"]==0


def test_max_error_is_reported_without_inventing_a_new_max_error_qualification_gate():
    r=_match(_diagnostics([.5]*100),_diagnostics([.7,.3]+[.5]*98))
    assert r["status"]=="MATCHED" and r["max_absolute_gap"]==.2
    assert r["p95_absolute_gap"]==0 and r["mean_signed_gap"]==0
    assert r["mean_absolute_gap"]==.004 and r["gap_RMSE"]==pytest.approx((.08/100)**.5)
    assert len(r["above_5pp_dates"])==2 and r["longest_known_above_5pp_run"]==2


def test_unknown_date_breaks_persistent_large_gap_run_instead_of_bridging_it():
    r=_match(_diagnostics([.5]*10),_diagnostics([.56]*10,unknown=[3,7]))
    assert r["longest_known_above_5pp_run"]==3 and len(r["above_5pp_dates"])==8
    assert len(r["unknown_dates"])==2 and r["known_exposure_coverage"]==.8


def test_no_known_pairs_means_none_statistics_not_fake_zero_matching():
    r=_match(_diagnostics([.8]*3,unknown=[0,1,2]),_diagnostics([.8]*3))
    assert r["status"]=="UNKNOWN_EXPOSURE_COVERAGE" and r["passed"] is False
    assert r["known_exposure_days"]==0 and r["daily_gaps"]==[]
    assert all(r[k] is None for k in ("mean_signed_gap","mean_absolute_gap","p95_absolute_gap","gap_RMSE","max_absolute_gap"))


@pytest.mark.parametrize("ref_valid,matched_valid",[(False,True),(True,False),(False,False),(1,True)])
def test_accounting_invalid_or_non_boolean_truthy_paths_never_become_matched(ref_valid,matched_valid):
    r=_match(_diagnostics([.8]*3),_diagnostics([.8]*3),reference_valid=ref_valid,matched_valid=matched_valid)
    assert r["passed"] is False and r["status"]=="INVALID_PATH"


@pytest.mark.parametrize("fault",["missing_middle","duplicate","out_of_order","different_date","empty_reference"])
def test_date_intersection_or_reordering_cannot_inflate_matching_coverage(fault):
    ref=_diagnostics([.8]*4);matched=deepcopy(ref)
    if fault=="missing_middle":matched["daily_exposures"].pop(1)
    elif fault=="duplicate":matched["daily_exposures"][1]=deepcopy(matched["daily_exposures"][0])
    elif fault=="out_of_order":matched["daily_exposures"].reverse()
    elif fault=="different_date":matched["daily_exposures"][-1]["date"]="2025-01-20"
    else:ref["daily_exposures"]=[]
    with pytest.raises(ValueError,match="same actual session"):_match(ref,matched)


def test_only_financially_stopped_path_may_have_a_prefix_and_its_missing_tail_stays_unknown():
    ref=_diagnostics([.8]*100);matched=_diagnostics([.8]*98)
    r=_match(ref,matched,matched_valid=False)
    assert r["status"]=="INVALID_PATH" and r["session_count"]==100 and r["known_exposure_days"]==98
    assert r["known_exposure_coverage"]==.98 and len(r["unknown_dates"])==2
    assert len(matched["daily_exposures"])==98  # no invented terminal prices or NAV rows.


@pytest.mark.parametrize("value",[None,True,float('nan'),float('inf'),-.01,"0.8"])
def test_claimed_known_risk_requires_finite_nonnegative_numeric_exposure(value):
    ref=_diagnostics([.8]);matched=_diagnostics([.8]);matched["daily_exposures"][0]["risky_nav_fraction"]=value
    with pytest.raises(ValueError,match="finite nonnegative"):_match(ref,matched)


def _paths():
    return [{"arm_key":arm,"period":period,"performance_valid":True,"metrics":{
        **_metrics(),"total_return_lower_exact":{"hold10_cap2":".12345678901234567890",
            "cap7p5pct":".20000000000000000001","matched_original":".15000000000000000003"}[arm]}}
        for arm in ARMS for period in PERIODS]


def _matching():return {period:{"status":"MATCHED","passed":True} for period in PERIODS}


def test_signed_net_bridge_adds_exactly_but_never_claims_pure_cash_or_turnover_causality():
    r=experiment.decide_exposure_matching(_paths(),_matching())
    assert r["status"]=="MATCHED_DESCRIPTIVE_COMPARISON"
    for p in r["periods"].values():
        assert p["eligible_for_descriptive_comparison"] is True
        bridge=p["net_return_bridge"]
        assert Decimal(bridge["cash_control_related_net_change_including_cost_and_path"])+Decimal(bridge["remaining_cap75_minus_matched_net_gap"])==Decimal(bridge["raw_cap75_minus_original_net"])
        assert bridge["pure_cash_or_turnover_causal_claim"] is False
        assert not any('drawdown' in k for k in bridge)
    assert r["current_paper_action"]=="keep_existing_top20_unchanged"
    assert r["strategy_promoted"] is r["model_retrained"] is r["new_future_OOS_claim"] is False


@pytest.mark.parametrize("arm",ARMS)
def test_any_invalid_arm_blocks_its_period_attribution_without_hiding_other_periods(arm):
    paths=_paths();next(r for r in paths if r["arm_key"]==arm and r["period"]=="continuous")["performance_valid"]=False
    r=experiment.decide_exposure_matching(paths,_matching())
    assert r["status"]=="INCONCLUSIVE_INVALID_ORIGINAL_REFERENCE"
    assert r["periods"]["continuous"]["net_return_bridge"] is None
    assert r["periods"]["2025"]["net_return_bridge"] is not None


@pytest.mark.parametrize("status",["INVALID_PATH","UNKNOWN_EXPOSURE_COVERAGE","MATCHING_FAILED"])
def test_failure_or_unknown_matching_does_not_produce_descriptive_attribution(status):
    matching=_matching();matching["2026"]={"status":status,"passed":False}
    r=experiment.decide_exposure_matching(_paths(),matching)
    assert r["status"]=="MATCHING_NOT_PROVEN" and r["periods"]["2026"]["net_return_bridge"] is None


@pytest.mark.parametrize("fault",["missing_path","duplicate_path","unknown_arm","missing_period_gate","gate_status_conflict","truthy_gate","truthy_validity"])
def test_nine_unique_fixed_paths_and_explicit_matching_validity_are_required(fault):
    paths=_paths();matching=_matching()
    if fault=="missing_path":paths.pop()
    elif fault=="duplicate_path":paths.append(deepcopy(paths[0]))
    elif fault=="unknown_arm":paths[-1]["arm_key"]="posthoc_best"
    elif fault=="missing_period_gate":matching.pop("2025")
    elif fault=="gate_status_conflict":matching["2025"]["passed"]=False
    elif fault=="truthy_gate":matching["2025"]["passed"]=1
    else:paths[-1]["performance_valid"]=1
    with pytest.raises(ValueError):experiment.decide_exposure_matching(paths,matching)


@pytest.mark.parametrize("field",["source","active_paper","reference_directory","parent_proof"])
def test_runtime_cannot_redirect_inherited_sources_or_unchanged_reference(declaration_case,field):
    c=declaration_case;_freeze(c);path=c.arguments["output"];declared=json.loads(path.read_bytes())
    if field=="source":declared["source"]["snapshot"]=str(c.tmp/"different_snapshot")
    elif field=="parent_proof":
        other=c.tmp/"different_protocol.json";_write(other,{"valid_bytes":"different frozen source"})
        declared["bound_inputs"]["protocol"]=experiment._proof(other)
    else:declared[field]=str(c.tmp/("different_"+field))
    _write(path,declared)
    with pytest.raises(ValueError):experiment.run_exposure_matching_comparison(path,c.tmp/"results")
    assert not (c.tmp/"results").exists() and len(c.calls)==1


@pytest.mark.parametrize("target",["a12_experiment","a12_validation","a12_execution","parent","design","reference","active_code"])
def test_changed_bound_bytes_stop_before_source_load_or_any_new_ledger(declaration_case,target):
    c=declaration_case;_freeze(c)
    path=c.codes[target] if target in c.codes else c.parent_path if target=="parent" else c.arguments["design_path"] if target=="design" else c.reference/"cap7p5pct/2026/capital_turnover.json" if target=="reference" else c.active_code
    path.write_text("modified after source freeze")
    with pytest.raises(ValueError):experiment.run_exposure_matching_comparison(c.arguments["output"],c.tmp/"results")
    assert not (c.tmp/"results").exists() and len(c.calls)==1


def test_new_runtime_implementation_path_cannot_replace_original_proof(declaration_case,monkeypatch):
    c=declaration_case;_freeze(c);different=c.tmp/"new_runtime.py";different.write_text("# changed implementation")
    monkeypatch.setattr(experiment,"implementation_proofs",lambda:{"a12_experiment":experiment._proof(different)})
    with pytest.raises(ValueError,match="implementation changed"):
        experiment.run_exposure_matching_comparison(c.arguments["output"],c.tmp/"results")
    assert not (c.tmp/"results").exists() and len(c.calls)==1


def test_offline_output_isolation_protects_active_paper_tree(declaration_case):
    c=declaration_case;_freeze(c);unsafe=c.paper/"offline-results"
    with pytest.raises(ValueError):
        experiment.freeze_exposure_matching_declaration(**{**c.arguments,"output":unsafe})
    with pytest.raises(ValueError):
        experiment.run_exposure_matching_comparison(c.arguments["output"],unsafe)
    assert not unsafe.exists()
    with pytest.raises(ValueError,match="separate"):
        experiment._check_output_isolation(c.tmp/"new_root",c.tmp/"new_root/paper")


class _VerifiedInput:
    def __init__(self,calls,name):self.calls,self.name=calls,name
    @contextmanager
    def verified_batch(self):
        self.calls.append(("batch_enter",self.name));yield self;self.calls.append(("batch_exit",self.name))
    def assert_unchanged(self):self.calls.append(("assert_unchanged",self.name))


@pytest.fixture
def synthetic_run(declaration_case,monkeypatch):
    c=declaration_case;_freeze(c)
    context={"bound":{},"pool":_VerifiedInput(c.calls,"pool"),"actions":_VerifiedInput(c.calls,"actions"),
        "score_coverage":{"old_cache":"must_not_enter_isolated_ledger"}}
    def load(*args):c.calls.append(("load",args));return context
    monkeypatch.setattr(experiment,"load_stability_context",load)
    def simulate(context,*,period,arm_key,reference_exposures):
        assert "score_coverage" not in context
        c.calls.append(("simulate",arm_key,period))
        assert reference_exposures=={r['date']:r for r in c.diagnostics['cap7p5pct',period]['daily_exposures']}
        result=deepcopy(c.references["hold10_cap2" if arm_key=="matched_original" else arm_key,period])
        if arm_key=="matched_original":
            result['records']['exposure_matching']=[{'session':'2026-10-19','known':True,'target_risk_fraction':'.8',
                'prior_reference_session':'2026-10-16','first_batch_exempt':False,'refused_second_pass':False}]
            for index,row in enumerate(result['records']['trades']):row['order_purpose']='core' if index==0 else 'cash_control'
        return result
    monkeypatch.setattr(experiment,"simulate_exposure_matching",simulate)
    original=experiment.assert_matching_reference_reproduced
    def reproduce(result,directory):
        receipt=original(result,directory);c.calls.append(("reproduced",Path(directory).parent.name,result['period']));return receipt
    monkeypatch.setattr(experiment,"assert_matching_reference_reproduced",reproduce)
    from stopat30m import ashare_position_count_metrics
    monkeypatch.setattr(ashare_position_count_metrics,"position_count_diagnostics",lambda result:_diagnostics([.8],start='2026-10-19'))
    c.context,c.simulate=context,simulate
    return c


def test_mock_orchestration_replays_six_originals_before_three_matched_paths_and_preserves_all_artifacts(synthetic_run):
    c=synthetic_run;before={p:p.read_bytes() for p in c.paper.iterdir()};progress=[]
    r=experiment.run_exposure_matching_comparison(c.arguments['output'],c.tmp/'results',progress=progress.append)
    assert [call[1:] for call in c.calls if call[0]=='simulate']==[(a,p) for a in ARMS for p in PERIODS]
    first_matched=c.calls.index(('simulate','matched_original','2025'))
    assert len([call for call in c.calls[:first_matched] if call[0]=='reproduced'])==6
    assert set(r['baseline_full_reproduction'])=={a+'/'+p for a in ARMS[:2] for p in PERIODS}
    for key,receipt in r['baseline_full_reproduction'].items():
        assert receipt['passed'] is True
        assert len(receipt['record_types'])==(13 if key.startswith('hold10_cap2/') else 14)
    assert len(r['results'])==9 and set(r['matching_by_period'])==set(PERIODS)
    assert r['matching_by_period']['continuous']['status']=='INVALID_PATH'
    assert r['decision']['status']=='INCONCLUSIVE_INVALID_ORIGINAL_REFERENCE'
    assert r['all_performance_valid'] is False and r['native_fits']==r['live_broker_orders']==0
    assert r['predictions_regenerated'] is r['active_paper_changed_by_experiment'] is False
    assert all(p.read_bytes()==raw for p,raw in before.items())
    out=c.tmp/'results';assert (out/'declaration.json').read_bytes()==c.arguments['output'].read_bytes()
    for arm in ARMS:
        for period in PERIODS:
            folder=out/arm/period;summary=json.loads((folder/'summary.json').read_bytes())
            assert summary['arm_key']==arm and 'daily_exposures' not in summary['diagnostics']
            assert json.loads((folder/'diagnostics.json').read_bytes())['daily_exposures']
            if arm=='matched_original':
                assert json.loads((folder/'exposure_matching.json').read_bytes())[0]['first_batch_exempt'] is False
                assert (folder/'matching.json').exists()
                assert summary['purpose_costs']['core']['fill_count']==summary['purpose_costs']['cash_control']['fill_count']==1
            else:
                assert not (folder/'exposure_matching.json').exists()
                assert summary['purpose_costs']['cash_control']['fill_count']==0
    manifest=json.loads((out/'manifest.json').read_bytes())
    assert set(manifest['files'])=={str(p.relative_to(out)) for p in out.rglob('*') if p.is_file() and p.name!='manifest.json'}
    assert all(hashlib.sha256((out/n).read_bytes()).hexdigest()==h for n,h in manifest['files'].items())
    assert len([call for call in c.calls if call[0]=='verify'])==3
    assert len([row for row in progress if row.get('completed')])==9
    assert c.context['score_coverage']=={'old_cache':'must_not_enter_isolated_ledger'}


@pytest.mark.parametrize('arm,record',[(arm,record) for arm in ARMS[:2] for record in
    ('curves','trades','orders','positions','claims','signals','corporate_actions','plans','valuation_audit',
     'score_coverage','holding_age','holding_origin_actions','turnover_plans')]+[('cap7p5pct','capital_turnover')])
def test_any_control_record_mismatch_prevents_all_new_matching_paths(synthetic_run,monkeypatch,arm,record):
    c=synthetic_run
    def bad(*args,**kwargs):
        result=c.simulate(*args,**kwargs)
        if kwargs['arm_key']==arm and kwargs['period']=='2026':result['records'][record][0]['changed_without_NAV_change']=True
        return result
    monkeypatch.setattr(experiment,'simulate_exposure_matching',bad)
    with pytest.raises(ValueError,match='Full original record'):
        experiment.run_exposure_matching_comparison(c.arguments['output'],c.tmp/'results')
    assert not any(call[:2]==('simulate','matched_original') for call in c.calls)
    failure=json.loads((c.tmp/'results/failure.json').read_bytes())
    assert failure['no_valid_comparison_claim'] is True and not (c.tmp/'results/report.json').exists()


@pytest.mark.parametrize('where',['metrics','final_state','record_inventory'])
def test_control_core_summary_final_claims_and_record_inventory_must_be_exact(synthetic_run,monkeypatch,where):
    c=synthetic_run
    def bad(*args,**kwargs):
        result=c.simulate(*args,**kwargs)
        if kwargs['arm_key']=='cap7p5pct' and kwargs['period']=='2025':
            if where=='metrics':result['metrics']['total_return_lower_exact']='9.0'
            elif where=='final_state':result['final_state']['claims']={}
            else:result['records'].pop('capital_turnover')
        return result
    monkeypatch.setattr(experiment,'simulate_exposure_matching',bad)
    with pytest.raises(ValueError):experiment.run_exposure_matching_comparison(c.arguments['output'],c.tmp/'results')
    assert not any(call[:2]==('simulate','matched_original') for call in c.calls)


def test_changed_runtime_dependency_is_detected_before_publishing_completed_results(synthetic_run):
    c=synthetic_run
    def progress(row):
        if row.get('completed') and row['arm_key']=='matched_original' and row['period']=='continuous':
            c.codes['a12_execution'].write_text('# changed source during synthetic run')
    with pytest.raises(ValueError):experiment.run_exposure_matching_comparison(c.arguments['output'],c.tmp/'results',progress=progress)
    assert len([call for call in c.calls if call[0]=='simulate'])==9
    assert not (c.tmp/'results/report.json').exists() and not (c.tmp/'results/manifest.json').exists()


def test_legitimate_live_state_advance_does_not_become_a_frozen_offline_dependency(synthetic_run):
    c=synthetic_run;_write(c.paper/'state.json',{'synthetic_next_session':'new receipt','cash':'14000.00'})
    r=experiment.run_exposure_matching_comparison(c.arguments['output'],c.tmp/'results')
    assert r['active_paper_changed_by_experiment'] is False
    assert json.loads((c.paper/'state.json').read_bytes())['cash']=='14000.00'


def test_core_and_cash_control_fees_are_sum_of_each_actual_fill_without_netting(declaration_case):
    c=declaration_case;result=deepcopy(c.references['hold10_cap2','2025'])
    result['records']['trades'][0]['order_purpose']='core'
    result['records']['trades'][1]['order_purpose']='cash_control'
    costs=experiment.purpose_costs(result,'matched_original')
    assert costs['core']['commission_cny']=='5.00' and costs['cash_control']['commission_cny']=='6.00'
    assert Decimal(costs['core']['total_fee_cny'])+Decimal(costs['cash_control']['total_fee_cny'])==Decimal(result['metrics']['total_fee_cny'])
    assert costs['core']['fill_count']==costs['cash_control']['fill_count']==1


@pytest.mark.parametrize('fault',['missing_purpose','unknown_purpose','fee_mismatch','slippage_mismatch'])
def test_cost_purpose_or_exact_ledger_total_mismatch_is_blocked(declaration_case,fault):
    c=declaration_case;result=deepcopy(c.references['hold10_cap2','2025'])
    for row in result['records']['trades']:row['order_purpose']='core'
    if fault=='missing_purpose':result['records']['trades'][0].pop('order_purpose')
    elif fault=='unknown_purpose':result['records']['trades'][0]['order_purpose']='netted_free'
    elif fault=='fee_mismatch':result['metrics']['total_fee_cny']='0'
    else:result['metrics']['slippage_cost_cny']='0'
    with pytest.raises(ValueError):experiment.purpose_costs(result,'matched_original')


def test_invalid_cli_result_exits_nonzero_once_without_switching_or_replaying_models(tmp_path,monkeypatch,capsys):
    rows=_paths();rows[-1]['performance_valid']=False
    decision=experiment.decide_exposure_matching(rows,_matching());calls=[]
    monkeypatch.setattr(experiment,'run_exposure_matching_comparison',lambda *args,**kwargs:calls.append(args) or {
        'all_performance_valid':False,'decision':decision})
    monkeypatch.setattr(sys,'argv',['a12','run','--declaration',str(tmp_path/'sealed.json'),'--output-dir',str(tmp_path/'unused')])
    with pytest.raises(SystemExit) as stopped:experiment.main()
    assert stopped.value.code==1 and len(calls)==1
    assert 'INCONCLUSIVE_INVALID_ORIGINAL_REFERENCE' in capsys.readouterr().out
    assert not (tmp_path/'unused').exists()
