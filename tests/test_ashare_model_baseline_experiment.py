"""Exact A14 replay gates and complete fixed-budget A16 accounting."""
from contextlib import nullcontext
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_model_baseline_experiment as ex
from stopat30m.ashare_turnover_experiment import JSON_RECORDS


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True))
    return path


def financial_row(strategy="original", family="primary", period="2023", *, valid=True, net="0.10", dd="-0.10"):
    return {"strategy":strategy,"family":family,"period":period,"performance_valid":valid,
        "metrics":{"total_return_lower_exact":net,"max_drawdown_conservative_exact":dd,
            "total_fee_cny":"100.00","slippage_cost_cny":"50.00","mean_invested_fraction":.9,
            "one_way_turnover_average_nav":4.0,"total_distribution_tax_reserved_cny":"10.00"}}


def canonical_result():
    row=financial_row();row.pop("strategy");row.pop("family")
    row.update(model="LGB",candidate="original",execution_id="E2",metrics_basis="declared_accounting_NAV_not_actual_liquidation",
        variant={"key":"hold10_cap2","minimum_hold_sessions":10,"max_replacements":2},
        score_quality={"summary":{"rank_IC":0.05}},
        records={"curves":[{"date":"2023-01-03","cash":"100000.00"}],
            "trades":[],"orders":[],"positions":[],"signals":[],"claims":[],
            **{name:[{"event":"original", "unchanged":True}] for name in JSON_RECORDS}},
        final_state={"cash":"100000.00","positions":{},"claims":{"pending":{"unknown":True}}})
    return row


def original_ledger(tmp_path):
    result=canonical_result();directory=tmp_path/"original";directory.mkdir()
    for name,rows in result["records"].items():
        if name in JSON_RECORDS:write(directory/(name+".json"),rows)
        else:pd.DataFrame(rows).to_csv(directory/(name+".csv"),index=False)
    write(directory/"final_state.json",result["final_state"])
    write(directory/"score_quality.json",result["score_quality"])
    summary={k:deepcopy(v) for k,v in result.items() if k not in {"records","final_state"}}
    summary["candidate"]="baseline"
    write(directory/"summary.json",summary)
    return result,directory


def test_original_candidate_marker_only_can_differ(tmp_path):
    result,directory=original_ledger(tmp_path)
    before={path:path.read_bytes() for path in directory.iterdir()}
    assert ex.assert_a14_reproduced(result,directory)["passed"] is True
    assert all(path.read_bytes()==raw for path,raw in before.items())


def test_invalid_original_path_must_reproduce_too_without_suppression(tmp_path):
    result,directory=original_ledger(tmp_path)
    result["performance_valid"]=False
    summary=json.loads((directory/"summary.json").read_text());summary["performance_valid"]=False
    write(directory/"summary.json",summary)
    assert ex.assert_a14_reproduced(result,directory)["passed"] is True


@pytest.mark.parametrize("fault",["net","drawdown","validity","period","model","variant","quality","extra_summary_field", "final_cash","pending_right", "csv_value","json_value","missing_record","extra_record"])
def test_any_original_economic_summary_or_record_mismatch_stops_replay(tmp_path,fault):
    result,directory=original_ledger(tmp_path)
    if fault=="net":result["metrics"]["total_return_lower_exact"]="0.10000000000000000001"
    elif fault=="drawdown":result["metrics"]["max_drawdown_conservative_exact"]="-0.10000000000000000001"
    elif fault=="validity":result["performance_valid"]=False
    elif fault=="period":result["period"]="2024"
    elif fault=="model":result["model"]="MOM60"
    elif fault=="variant":result["variant"]["minimum_hold_sessions"]=5
    elif fault=="quality":result["score_quality"]["summary"]["rank_IC"]=.9
    elif fault=="extra_summary_field":result["unfrozen_repair"]=True
    elif fault=="final_cash":result["final_state"]["cash"]="99999.99"
    elif fault=="pending_right":result["final_state"]["claims"]={}
    elif fault=="csv_value":result["records"]["curves"][0]["cash"]="99999.99"
    elif fault=="json_value":result["records"]["holding_age"][0]["unchanged"]=False
    elif fault=="missing_record":result["records"].pop("claims")
    else:result["records"]["unexpected_extra"]=[{"unknown":True}]
    with pytest.raises(ValueError):ex.assert_a14_reproduced(result,directory)


def test_csv_semantically_equal_but_different_serialization_is_not_exact_replay(tmp_path):
    result,directory=original_ledger(tmp_path)
    original=(directory/"curves.csv").read_text();(directory/"curves.csv").write_text(original.replace('100000.00','100000.000'))
    with pytest.raises(ValueError):ex.assert_a14_reproduced(result,directory)


def test_csv_record_order_is_part_of_exact_replay(tmp_path):
    result,directory=original_ledger(tmp_path)
    result["records"]["curves"].append({"date":"2023-01-04","cash":"99999.00"})
    pd.DataFrame(result["records"]["curves"][::-1]).to_csv(directory/"curves.csv",index=False)
    with pytest.raises(ValueError):ex.assert_a14_reproduced(result,directory)


def all_financial_rows():
    rows=[]
    for family,periods in ex.PERIODS.items():
        for period in periods:
            for strategy in ex.STRATEGIES:
                net={"original":"0.10","regularized300":"0.11","MOM60":"0.09","REV20":"0.12"}.get(strategy)
                dd={"original":"-0.10","regularized300":"-0.08","MOM60":"-0.11","REV20":"-0.09"}.get(strategy,"-0.10")
                if net is None:net=f"0.0{list(ex.STRATEGIES[4:]).index(strategy)+1}"
                rows.append(financial_row(strategy,family,period,net=net,dd=dd))
    return rows


def select_row(rows, strategy, *, family="primary", period="2023"):
    return next(row for row in rows if row["strategy"]==strategy and row["family"]==family and row["period"]==period)


def test_all_54_financial_paths_and_five_static_seeds_are_explicit_and_not_mutated():
    rows=all_financial_rows();before=deepcopy(rows)
    result=ex.summarize_financial_paths(rows)
    assert rows==before
    assert result["paths"]==result["financially_valid_paths"]==54
    assert len(result["paired_models_vs_simple"])==24
    assert len(result["fixed_hash_distributions"])==6
    for group in result["fixed_hash_distributions"]:
        assert group["fixed_seed_count"]==group["valid_count"]==5
        assert group["invalid_strategies"]==[] and group["all_five_valid"] is True
        assert group["conditional_on_valid_paths"] is False
        assert group["pvalue"] is None and group["best_seed_selected"] is False
        assert group["total_return_lower_exact"]==pytest.approx({"mean":.03,"median":.03,"min":.01,"max":.05})
    assert result["new_future_OOS_or_pure_alpha_claim"] is False
    assert result["retraining_decision_requires_joint_evidence"] is True


def test_same_period_model_control_deltas_are_paired_and_risk_matching_is_not_claimed():
    result=ex.summarize_financial_paths(all_financial_rows())
    pair=next(row for row in result["paired_models_vs_simple"]
        if (row["family"],row["period"],row["model"],row["control"])==("primary","2023","original","MOM60"))
    assert pair["comparison_allowed"] is True
    assert pair["net_return_difference"]==pytest.approx(.01)
    assert pair["drawdown_difference"]==pytest.approx(.01)
    assert pair["risk_and_exposure_matched"] is False


@pytest.mark.parametrize("strategy",["original","regularized300","MOM60","REV20"])
def test_invalid_model_or_simple_path_has_no_win_loss_deltas(strategy):
    rows=all_financial_rows();select_row(rows,strategy)["performance_valid"]=False
    result=ex.summarize_financial_paths(rows)
    assert result["financially_valid_paths"]==53
    for pair in result["paired_models_vs_simple"]:
        targeted=pair["family"]=="primary" and pair["period"]=="2023" and strategy in (pair["model"],pair["control"])
        if targeted:
            assert pair["comparison_allowed"] is False
            assert pair["net_return_difference"] is None and pair["drawdown_difference"] is None
        else:assert pair["comparison_allowed"] is True


def test_invalid_hash_seed_is_retained_and_conditional_denominator_disclosed():
    rows=all_financial_rows();bad=select_row(rows,"HASH1103");bad["performance_valid"]=False
    bad["metrics"]["total_return_lower_exact"]="999"
    result=ex.summarize_financial_paths(rows)
    group=next(row for row in result["fixed_hash_distributions"] if row["family"]=="primary" and row["period"]=="2023")
    assert group["fixed_seed_count"]==5 and group["valid_count"]==4
    assert group["invalid_strategies"]==["HASH1103"]
    assert group["all_five_valid"] is False and group["conditional_on_valid_paths"] is True
    assert group["total_return_lower_exact"]==pytest.approx({"mean":.035,"median":.035,"min":.02,"max":.05})
    assert group["best_seed_selected"] is False and group["pvalue"] is None


def test_all_invalid_hashes_have_empty_statistics_not_zero_or_a_selected_winner():
    rows=all_financial_rows()
    for strategy in ex.STRATEGIES[4:]:select_row(rows,strategy)["performance_valid"]=False
    result=ex.summarize_financial_paths(rows)
    group=next(row for row in result["fixed_hash_distributions"] if row["family"]=="primary" and row["period"]=="2023")
    assert group["valid_count"]==0 and len(group["invalid_strategies"])==5
    assert group["conditional_on_valid_paths"] is True and group["all_five_valid"] is False
    for key in ("total_return_lower_exact","max_drawdown_conservative_exact"):
        assert group[key]==dict.fromkeys(["mean","median","min","max"])
    assert result["financially_valid_paths"]==49


@pytest.mark.parametrize("fault",["missing_model","missing_seed","duplicate","extra_seed","wrong_family","wrong_period","unknown_model"])
def test_financial_summary_rejects_missing_duplicate_or_unfrozen_paths(fault):
    rows=all_financial_rows()
    if fault=="missing_model":rows.pop(0)
    elif fault=="missing_seed":rows.remove(select_row(rows,"HASH5519"))
    elif fault=="duplicate":rows.append(deepcopy(rows[0]))
    elif fault=="extra_seed":rows.append({**deepcopy(rows[-1]),"strategy":"HASH9999"})
    elif fault=="wrong_family":rows[0]["family"]="mixed"
    elif fault=="wrong_period":rows[0]["period"]="2027"
    else:rows[0]["strategy"]="H20"
    with pytest.raises(ValueError):ex.summarize_financial_paths(rows)


@pytest.mark.parametrize("flag",[0,1,None,"true"])
def test_financial_validity_is_an_exact_boolean(flag):
    rows=all_financial_rows();rows[0]["performance_valid"]=flag
    with pytest.raises(ValueError):ex.summarize_financial_paths(rows)


@pytest.mark.parametrize("value",["NaN","Infinity","-Infinity",None,True])
@pytest.mark.parametrize("key",["total_return_lower_exact","max_drawdown_conservative_exact"])
def test_nonfinite_or_non_numeric_financial_metrics_cannot_be_summarized(key,value):
    rows=all_financial_rows();rows[0]["metrics"][key]=value
    with pytest.raises((ValueError,ArithmeticError,TypeError)):
        ex.summarize_financial_paths(rows)


def write_reference(directory,result,candidate):
    directory.mkdir(parents=True,exist_ok=False)
    for name,rows in result["records"].items():
        if name in JSON_RECORDS:write(directory/(name+".json"),rows)
        else:pd.DataFrame(rows).to_csv(directory/(name+".csv"),index=False)
    write(directory/"final_state.json",result["final_state"])
    summary={k:deepcopy(v) for k,v in result.items() if k not in {"records","final_state","candidate"}}
    summary["candidate"]=candidate
    write(directory/"summary.json",summary)


def isolated_runner(tmp_path,monkeypatch,*,invalid_paths=()):
    paper=tmp_path/"active";paper.mkdir()
    write(paper/"manifest.json",{"model":"fixed-native","positions":20})
    write(paper/"state.json",{"cash":"14258.12","positions":{"SH600000":100}})
    before={p:p.read_bytes() for p in paper.iterdir()}
    declaration={"kind":ex.KIND,"active_paper_directory":str(paper),"training_declaration":"synthetic-training.json",
        "packages":{phase:str(tmp_path/phase) for phase in ("selection","confirmation","retrospective")},
        "bound_inputs":{}}
    signal_reference=tmp_path/"old_signal_daily.csv";signal_reference.write_text("signal_date,group\nsynthetic,union\n")
    declaration["bound_inputs"]["original_signal:signal_daily.csv"]=ex._proof(signal_reference)
    declaration_path=write(tmp_path/"declaration.json",declaration)
    monkeypatch.setattr(ex,"_load_declaration",lambda _:deepcopy(declaration))
    references={};values={};trace=[]
    for family,periods in ex.PERIODS.items():
        for period in periods:
            for strategy in ex.MODELS:
                result=canonical_result();result["period"]=period
                result["performance_valid"]=(family,period,strategy) not in invalid_paths
                if strategy=="regularized300":result.pop("score_quality")
                directory=tmp_path/"references"/family/strategy/period
                write_reference(directory,result,"baseline" if strategy=="original" else strategy)
                references[family,period,strategy]=directory
                values[family,period,strategy]=result
    monkeypatch.setattr(ex,"_reference",lambda family,period,strategy:references[family,period,strategy])
    frozen=tmp_path/"original-source.py";frozen.write_text("# unchanged original source\n")
    contexts={family:{"family":family,"pool":SimpleNamespace(verified_batch=nullcontext,assert_unchanged=lambda:None),
        "actions":SimpleNamespace(assert_unchanged=lambda:None),"bound":{"source":ex._proof(frozen)},
        "scores":{"LGB":pd.Series([1.0],index=pd.MultiIndex.from_tuples([(pd.Timestamp("2023-01-03"),"SH600000")],names=["datetime","instrument"]))}}
        for family in ex.PERIODS}
    monkeypatch.setattr(ex.validation,"load_baseline_context",lambda _,family:contexts[family])
    def simulate(context,*,strategy,period,prediction_package=None):
        family=context["family"];trace.append(("simulate",family,period,strategy,prediction_package))
        if strategy in ex.MODELS:return deepcopy(values[family,period,strategy])
        result=canonical_result();result.pop("score_quality");result["period"]=period
        result["performance_valid"]=(family,period,strategy) not in invalid_paths
        return result
    monkeypatch.setattr(ex.validation,"simulate_baseline_strategy",simulate)
    monkeypatch.setattr(ex.stability,"score_quality",lambda *_,**__:canonical_result()["score_quality"])
    real_assert=ex.assert_a14_reproduced
    def assert_replay(result,directory):
        outcome=real_assert(result,directory);trace.append(("verified",str(directory)))
        return outcome
    monkeypatch.setattr(ex,"assert_a14_reproduced",assert_replay)
    def score_series(context,strategy,*,period,prediction_package=None):
        trace.append(("score_read",context["family"],period,strategy,prediction_package))
        return pd.Series([1.0],index=pd.MultiIndex.from_tuples([(pd.Timestamp("2023-01-03"),"SH600000")],names=["datetime","instrument"]))
    monkeypatch.setattr(ex.validation,"baseline_signal_scores",score_series)
    def diagnose(context,*,score_sets,periods,original_key="original"):
        trace.append(("diagnosis",context["family"],tuple(score_sets),tuple(periods),original_key))
        return {"daily":pd.DataFrame({"signal_date":["synthetic"],"unknown":[None]}),"summary":[],"paired":[]}
    monkeypatch.setattr(ex.signal,"diagnose_scores",diagnose)
    def signal_quality(*_,period):
        trace.append(("signal_verified",period));return {"passed":True}
    monkeypatch.setattr(ex.signal,"assert_quality_reference",signal_quality)
    monkeypatch.setattr(ex.signal,"assert_original_reference",lambda *_:{"passed":True,"rows":844})
    def prohibited(*_,**__):raise AssertionError("No training, native prediction or live paper phase is permitted")
    monkeypatch.setattr(ex.training,"build_debug_predictions",prohibited)
    return {"declaration_path":declaration_path,"declaration":declaration,"trace":trace,"paper_before":before,
        "source":frozen,"contexts":contexts,"references":references,"values":values}


def test_all_twelve_replays_finish_before_any_of_the_42_controls(tmp_path,monkeypatch):
    fixture=isolated_runner(tmp_path,monkeypatch)
    report=ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    calls=[row for row in fixture["trace"] if row[0]=="simulate"]
    assert len(calls)==len(report["results"])==54
    assert len({row[1:4] for row in calls})==54
    assert all(row[3] in ex.MODELS for row in calls[:12])
    assert all(row[3] not in ex.MODELS for row in calls[12:])
    first_control=next(i for i,row in enumerate(fixture["trace"]) if row[0]=="simulate" and row[3] not in ex.MODELS)
    assert sum(row[0]=="verified" for row in fixture["trace"][:first_control])==12
    gate=json.loads((tmp_path/"out/all12_reproduced_before_controls.json").read_text())
    assert len(gate["reproductions"])==12 and gate["new_control_ledgers"]==0
    assert all(item["passed"] for item in gate["reproductions"].values())
    assert report["new_native_fits"]==report["new_native_predictions"]==report["broker_orders"]==0
    assert report["active_paper_mutated_by_experiment"] is False and report["automatic_model_promotion"] is False
    assert report["future_OOS_claim"] is False and report["all_historical_periods_previously_viewed"] is True
    assert all(path.read_bytes()==raw for path,raw in fixture["paper_before"].items())
    assert (tmp_path/"out/report.json").is_file() and (tmp_path/"out/manifest.json").is_file()


def test_regularized_packages_route_by_source_family_and_carry_both_primary_years(tmp_path,monkeypatch):
    fixture=isolated_runner(tmp_path,monkeypatch)
    ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    calls={(row[1],row[2],row[3]):row[4] for row in fixture["trace"] if row[0]=="simulate"}
    packages={key:Path(value) for key,value in fixture["declaration"]["packages"].items()}
    assert calls["primary","2023","regularized300"]==packages["selection"]
    assert calls["primary","2024","regularized300"]==packages["confirmation"]
    assert calls["primary","continuous","regularized300"]==[packages["selection"],packages["confirmation"]]
    for period in ex.PERIODS["stability"]:assert calls["stability",period,"regularized300"]==packages["retrospective"]
    assert all(value is None for key,value in calls.items() if key[2]!="regularized300")


@pytest.mark.parametrize("bad_index",[0,5,6,11])
def test_any_failed_replay_including_twelfth_prevents_every_new_benchmark(tmp_path,monkeypatch,bad_index):
    fixture=isolated_runner(tmp_path,monkeypatch);real_assert=ex.assert_a14_reproduced;counter=[]
    def reject(result,directory):
        counter.append(directory)
        if len(counter)-1==bad_index:raise ValueError("synthetic exact replay disagreement")
        return real_assert(result,directory)
    monkeypatch.setattr(ex,"assert_a14_reproduced",reject)
    with pytest.raises(ValueError,match="exact replay disagreement"):
        ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    calls=[row for row in fixture["trace"] if row[0]=="simulate"]
    assert len(calls)==bad_index+1 and all(row[3] in ex.MODELS for row in calls)
    failed=json.loads((tmp_path/"out/failure.json").read_text())
    assert failed["completed_ledgers"]==bad_index and failed["no_new_model_or_promotion_claim"] is True
    assert not (tmp_path/"out/all12_reproduced_before_controls.json").exists()
    assert not (tmp_path/"out/report.json").exists()


def test_invalid_reproduced_old_and_new_random_paths_are_all_saved_and_finish(tmp_path,monkeypatch):
    invalid={("stability","continuous","original"),("primary","2024","HASH1103")}
    fixture=isolated_runner(tmp_path,monkeypatch,invalid_paths=invalid)
    report=ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    assert len(report["results"])==54 and report["financial_summary"]["financially_valid_paths"]==52
    for family,period,strategy in invalid:
        summary=json.loads((tmp_path/"out"/family/strategy/period/"summary.json").read_text())
        final=json.loads((tmp_path/"out"/family/strategy/period/"final_state.json").read_text())
        assert summary["performance_valid"] is False
        assert final["claims"]=={"pending":{"unknown":True}}
    assert not (tmp_path/"out/failure.json").exists()


def test_source_change_after_model_replay_stops_controls_without_success_report(tmp_path,monkeypatch):
    fixture=isolated_runner(tmp_path,monkeypatch);real_sim=ex.validation.simulate_baseline_strategy
    def simulate(context,*,strategy,period,prediction_package=None):
        value=real_sim(context,strategy=strategy,period=period,prediction_package=prediction_package)
        if context["family"]=="primary" and strategy=="HASH5519" and period=="continuous":fixture["source"].write_text("# corrupted source")
        return value
    monkeypatch.setattr(ex.validation,"simulate_baseline_strategy",simulate)
    with pytest.raises(ValueError):ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    assert (tmp_path/"out/failure.json").exists() and not (tmp_path/"out/report.json").exists()
    calls=[row for row in fixture["trace"] if row[0]=="simulate"]
    assert not any(row[1]=="stability" and row[3] not in ex.MODELS for row in calls)
    assert all(path.read_bytes()==raw for path,raw in fixture["paper_before"].items())


def test_final_declaration_recheck_failure_retains_ledgers_without_promotion_claim(tmp_path,monkeypatch):
    fixture=isolated_runner(tmp_path,monkeypatch);loads=[]
    def load(_):
        loads.append(True)
        if len(loads)>1:raise ValueError("final frozen source proof drift")
        return deepcopy(fixture["declaration"])
    monkeypatch.setattr(ex,"_load_declaration",load)
    with pytest.raises(ValueError,match="source proof drift"):
        ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    assert len([row for row in fixture["trace"] if row[0]=="simulate"])==54
    assert (tmp_path/"out/failure.json").is_file() and not (tmp_path/"out/report.json").exists()


@pytest.mark.parametrize("kind",["same","child","parent","models","forward","existing"])
def test_output_path_cannot_overwrite_paper_models_or_previous_results(tmp_path,kind):
    paper=tmp_path/"paper";paper.mkdir()
    targets={"same":paper,"child":paper/"new","parent":tmp_path,"models":tmp_path/"models/new",
        "forward":tmp_path/"forward/new","existing":tmp_path/"previous"}
    if kind=="existing":targets[kind].mkdir()
    with pytest.raises((ValueError,FileExistsError)):ex._isolated(targets[kind],paper)


def historical_fixture(tmp_path,monkeypatch):
    # Reuse the real isolated A15 migration fixture, not an invented event shape.
    from test_ashare_low_turnover_recovery import case as recovery_case, migrate
    case=recovery_case.__wrapped__(tmp_path,monkeypatch)
    old_proof=ex._proof(case["root"]/"manifest.json")
    data={"active_paper_directory":str(case["root"]),"active_paper":old_proof,
        "bound_inputs":{"active_paper_manifest":old_proof,"immutable_training_asset":ex._proof(tmp_path/"native.txt")}}
    declaration=write(tmp_path/"a14-training.json",data)
    migrate(case)
    monkeypatch.setattr(ex.training,"_load_declaration",lambda path:(json.loads(Path(path).read_text()),{}))
    return case,declaration


def test_exact_a15_archive_rebinds_only_old_live_manifest_without_rewriting_a14(tmp_path,monkeypatch):
    case,path=historical_fixture(tmp_path,monkeypatch)
    before={p:p.read_bytes() for p in [path,case["root"]/"manifest.json",case["root"]/"state.json",case["before"]/"paper/manifest.json"]}
    data,binding=ex._verify_historical_declaration(path)
    assert data["active_paper"]["sha256"]!=ex._proof(case["root"]/"manifest.json")["sha256"]
    assert binding["historical_active_manifest"]==ex._proof(case["before"]/"paper/manifest.json")
    assert binding["historical_active_manifest"]["sha256"]==data["active_paper"]["sha256"]
    assert binding["current_active_manifest"]==ex._proof(case["root"]/"manifest.json")
    assert binding["unchanged_original_source_proofs_checked"]==1
    assert all(p.read_bytes()==raw for p,raw in before.items())


@pytest.mark.parametrize("fault",["ambiguous_oldproof","wrong_oldpath","archive_bytes","current_model","current_implementation","unknown_training_asset","missing_revision","foreign_event"])
def test_a15_compatibility_cannot_excuse_any_other_source_or_event_drift(tmp_path,monkeypatch,fault):
    from stopat30m import ashare_low_turnover_recovery as recovery
    case,path=historical_fixture(tmp_path,monkeypatch)
    data=json.loads(path.read_text());manifest=json.loads((case["root"]/"manifest.json").read_text())
    if fault=="ambiguous_oldproof":data["active_paper"]={**data["active_paper"],"sha256":"0"*64};write(path,data)
    elif fault=="wrong_oldpath":
        proof={**data["active_paper"],"path":str(tmp_path/"native.txt")};data["active_paper"]=proof;data["bound_inputs"]["active_paper_manifest"]=proof;write(path,data)
    elif fault=="archive_bytes":(case["before"]/"paper/manifest.json").write_text("{}")
    elif fault=="current_model":manifest["model_sha256"]="0"*64;write(case["root"]/"manifest.json",manifest)
    elif fault=="current_implementation":case["code"].write_text("unexpected engineering implementation")
    elif fault=="unknown_training_asset":(tmp_path/"native.txt").write_text("unexpected native training artifact")
    elif fault=="missing_revision":manifest.pop("engineering_revision");write(case["root"]/"manifest.json",manifest)
    else:
        event=Path(manifest["engineering_revision"]["event"]["path"]);value=json.loads(event.read_text());value["kind"]="foreign_event";write(event,value)
    with pytest.raises(ValueError):ex._verify_historical_declaration(path)


def test_mutable_daily_attempt_state_is_not_frozen_into_long_running_offline_experiment(tmp_path,monkeypatch):
    from stopat30m import ashare_forward as legacy
    case,path=historical_fixture(tmp_path,monkeypatch)
    _,before=ex._verify_historical_declaration(path)
    state_path=case["root"]/"state.json";state=json.loads(state_path.read_text())
    state["attempts"]["2026-10-22_close"]=1
    state["state_sha256"]=legacy._digest({k:v for k,v in state.items() if k!="state_sha256"})
    write(state_path,state)
    _,after=ex._verify_historical_declaration(path)
    assert before==after
    assert not any(isinstance(value,dict) and value.get("path")==str(state_path) for value in after.values())


def freeze_fixture(tmp_path,monkeypatch):
    source_path=write(tmp_path/"training.json",{"synthetic":"immutable original A14 declaration"})
    paper=tmp_path/"active";paper.mkdir();write(paper/"state.json",{"daily_attempts":0})
    current=write(paper/"manifest.json",{"model":"fixed"})
    old=write(tmp_path/"archived-manifest.json",{"model":"fixed"})
    event=write(tmp_path/"engineering-event.json",{"kind":"synthetic_recovery_boundary"})
    historical={"historical_active_manifest":ex._proof(old),"current_active_manifest":ex._proof(current),"paper_revision":ex._proof(event),"unchanged_original_source_proofs_checked":1}
    training={"active_paper_directory":str(paper)}
    monkeypatch.setattr(ex,"_verify_historical_declaration",lambda path:(deepcopy(training),deepcopy(historical)))
    code=tmp_path/"baseline-experiment.py";code.write_text("# frozen isolated implementation")
    monkeypatch.setattr(ex,"implementation_proofs",lambda:{"experiment":ex._proof(code)})
    packages={phase:tmp_path/"packages"/phase for phase in ("selection","confirmation","retrospective")}
    for phase,package in packages.items():write(package/"manifest.json",{"phase":phase,"files":{"native.txt":"synthetic"}})
    monkeypatch.setattr(ex,"_packages",lambda:packages)
    verifications=[]
    def verify(package,path):
        verifications.append((Path(package),Path(path)));return {"phase":Path(package).name}
    monkeypatch.setattr(ex.training,"verify_debug_model_package",verify)
    references={}
    for family,periods in ex.PERIODS.items():
        for period in periods:
            for strategy in ex.MODELS:
                result=canonical_result();result["records"]["corporate_actions"]=[];result["period"]=period
                directory=tmp_path/"references"/family/strategy/period
                write_reference(directory,result,strategy)
                references[family,period,strategy]=directory
    monkeypatch.setattr(ex,"_reference",lambda family,period,strategy:references[family,period,strategy])
    monkeypatch.setattr(ex,"BASE",tmp_path/"source-root")
    diagnostic=ex.BASE/"experiments/a14_model_diagnostics_20261021_v1";diagnostic.mkdir(parents=True)
    csv=diagnostic/"signal_daily.csv";csv.write_text("signal_date,group\nsynthetic,union\n")
    write(diagnostic/"signal_completion_manifest.json",{"files":{"signal_daily.csv":ex._proof(csv)["sha256"]}})
    design=json.loads((Path(__file__).parents[1]/"docs/research/a16_model_diagnosis_design_v1.json").read_text())
    design["source_training_declaration"]=str(source_path)
    # All test mechanics stay real, except the source path in this isolated design.
    monkeypatch.setattr(ex,"DESIGN_SHA256",ex.hashlib.sha256(ex._canonical(design)).hexdigest())
    design_path=write(tmp_path/"design.json",design)
    return {"design_path":design_path,"source_path":source_path,"paper":paper,"packages":packages,
        "references":references,"verifications":verifications,"code":code}


def test_freeze_binds_three_packages_twelve_complete_ledgers_and_historical_revision(tmp_path,monkeypatch):
    fixture=freeze_fixture(tmp_path,monkeypatch);output=tmp_path/"execution.json"
    declaration=ex.freeze_baseline_declaration(fixture["design_path"],output)
    assert len(fixture["verifications"])==3
    assert {p.name for p,_ in fixture["verifications"]}=={"selection","confirmation","retrospective"}
    assert declaration["new_native_fits"]==declaration["new_native_predictions"]==declaration["broker_orders"]==0
    assert declaration["new_returns_before_declaration"] is False
    assert declaration["bound_inputs"]["a14_training_declaration"]==ex._proof(fixture["source_path"])
    assert len([key for key in declaration["bound_inputs"] if key.startswith("reference:") and key.endswith(":summary.json")])==12
    assert not any(Path(value["path"]).name=="state.json" for value in declaration["bound_inputs"].values())
    assert ex._load_declaration(output)==declaration
    assert len(fixture["verifications"])==6  # Every later read verifies complete package inventories again.


@pytest.mark.parametrize("missing",["summary.json","final_state.json","trades.csv","claims.csv","valuation_audit.json","holding_origin_actions.json"])
def test_missing_any_critical_old_ledger_file_is_rejected_before_freeze(tmp_path,monkeypatch,missing):
    fixture=freeze_fixture(tmp_path,monkeypatch)
    directory=fixture["references"]["stability","continuous","original"]
    (directory/missing).unlink()
    output=tmp_path/"execution.json"
    with pytest.raises(ValueError):ex.freeze_baseline_declaration(fixture["design_path"],output)
    assert not output.exists()


@pytest.mark.parametrize("fault",["source_pointer","package_pointer","missing_package","active_directory","precomputed_returns","fit","predict","broker","implementation_bytes","old_ledger_bytes","design_bytes","revision_binding"])
def test_frozen_execution_pointer_phase_source_and_archive_guards_are_not_decoupled(tmp_path,monkeypatch,fault):
    fixture=freeze_fixture(tmp_path,monkeypatch);output=tmp_path/"execution.json"
    declaration=ex.freeze_baseline_declaration(fixture["design_path"],output)
    if fault=="source_pointer":declaration["training_declaration"]=str(write(tmp_path/"other-training.json",{"different":True}))
    elif fault=="package_pointer":
        other=tmp_path/"other-models";write(other/"manifest.json",{"different":True});declaration["packages"]["selection"]=str(other)
    elif fault=="missing_package":declaration["packages"].pop("retrospective")
    elif fault=="active_directory":declaration["active_paper_directory"]=str(tmp_path/"foreign-active")
    elif fault=="precomputed_returns":declaration["new_returns_before_declaration"]=True
    elif fault in {"fit","predict","broker"}:declaration[{"fit":"new_native_fits","predict":"new_native_predictions","broker":"broker_orders"}[fault]]=1
    elif fault=="implementation_bytes":fixture["code"].write_text("# changed after freeze")
    elif fault=="old_ledger_bytes":(fixture["references"]["primary","2023","original"]/"claims.csv").write_text("silently erased claim")
    elif fault=="design_bytes":fixture["design_path"].write_text("{}")
    else:declaration["historical_paper_binding"]["unchanged_original_source_proofs_checked"]+=1
    write(output,declaration)
    with pytest.raises(ValueError):ex._load_declaration(output)


def test_freeze_does_not_replace_an_existing_declaration(tmp_path,monkeypatch):
    fixture=freeze_fixture(tmp_path,monkeypatch);output=tmp_path/"execution.json";output.write_text("original output")
    with pytest.raises(FileExistsError):ex.freeze_baseline_declaration(fixture["design_path"],output)
    assert output.read_text()=="original output"


def test_frozen_design_cannot_expand_or_change_any_rules():
    design=json.loads((Path(__file__).parents[1]/"docs/research/a16_model_diagnosis_design_v1.json").read_text())
    ex._check_design(design)
    design["strategies"].append("HASH9999")
    with pytest.raises(ValueError):ex._check_design(design)


@pytest.mark.parametrize("family",["primary","stability"])
def test_original_signal_diagnostic_mismatch_stops_every_new_control(tmp_path,monkeypatch,family):
    fixture=isolated_runner(tmp_path,monkeypatch)
    def reject(*_,period):
        if period in ex.PERIODS[family]:raise ValueError("original signal diagnostic mismatch")
        return {"passed":True}
    monkeypatch.setattr(ex.signal,"assert_quality_reference",reject)
    with pytest.raises(ValueError,match="signal diagnostic mismatch"):
        ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    calls=[row for row in fixture["trace"] if row[0]=="simulate"]
    assert len(calls)==12 and all(row[3] in ex.MODELS for row in calls)
    assert (tmp_path/"out/all12_reproduced_before_controls.json").is_file()
    assert not (tmp_path/"out/original_signal_reproduction.json").exists()
    assert not (tmp_path/"out/report.json").exists()


def test_freeze_rejects_valid_package_manifest_routed_under_another_phase(tmp_path,monkeypatch):
    fixture=freeze_fixture(tmp_path,monkeypatch)
    write(fixture["packages"]["selection"]/"manifest.json",{"phase":"retrospective","files":{}})
    with pytest.raises(ValueError):ex.freeze_baseline_declaration(fixture["design_path"],tmp_path/"execution.json")


def test_loaded_declaration_rechecks_manifest_phase_even_when_its_bytes_are_bound(tmp_path,monkeypatch):
    fixture=freeze_fixture(tmp_path,monkeypatch);output=tmp_path/"execution.json"
    declaration=ex.freeze_baseline_declaration(fixture["design_path"],output)
    manifest=fixture["packages"]["confirmation"]/"manifest.json"
    write(manifest,{"phase":"selection","files":{}})
    declaration["bound_inputs"]["models:confirmation"]=ex._proof(manifest)
    write(output,declaration)
    with pytest.raises(ValueError):ex._load_declaration(output)


def test_original_844_group_day_reference_failure_blocks_all_new_controls(tmp_path,monkeypatch):
    fixture=isolated_runner(tmp_path,monkeypatch)
    def reject(*_):raise ValueError("original 844 group-day reference mismatch")
    monkeypatch.setattr(ex.signal,"assert_original_reference",reject)
    with pytest.raises(ValueError,match="844 group-day"):
        ex.run_baseline_comparison(fixture["declaration_path"],tmp_path/"out")
    calls=[row for row in fixture["trace"] if row[0]=="simulate"]
    assert len(calls)==12 and all(row[3] in ex.MODELS for row in calls)
    assert not (tmp_path/"out/original_signal_reproduction.json").exists()
    assert not (tmp_path/"out/report.json").exists()
