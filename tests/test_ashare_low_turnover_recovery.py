"""Recovery authorization and account preservation, using only local fixtures."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import pytest

from stopat30m import ashare_forward as legacy
from stopat30m import ashare_low_turnover_recovery as recovery
from stopat30m.backtest import ashare_execution as execution


NOW = datetime(2026, 10, 22, 8, tzinfo=timezone.utc)


def save(path, value):
    legacy._write(path, value)
    return recovery._proof(path)


@pytest.fixture
def case(tmp_path, monkeypatch):
    root, before = tmp_path / "paper", tmp_path / "before"
    root.mkdir(); archived = before / "paper"; archived.mkdir(parents=True)
    code = tmp_path / "engine.py"; code.write_text("old engineering source\n")
    source = before / "sources" / "engine.py"; source.parent.mkdir(); shutil.copyfile(code, source)
    monkeypatch.setattr(recovery, "_UPDATABLE", {code})
    native = tmp_path / "native.txt"; native.write_text("unchanged original native model")
    pool = [f"SH{600000+i:06d}" for i in range(1300)]
    from stopat30m.data.research_calendar import Calendar
    Calendar.from_rows([{'date':f'2026-10-{day}', 'is_open':True} for day in [20,21,22,23]],
        '2026-10-20','2026-10-23',source='synthetic',source_url='fixture://explicit-session-flags').save(root/'calendar.json')
    save(root / "universe.json", {"instruments":pool})
    save(root / "adapter.json", {"source":"fixture"})
    manifest = {"schema_version":1,"kind":"isolated_ashare_low_turnover_forward",
        "policy":"a9-prospective-frozen-a5-H5-hold10-cap2-v1","strategies":["model"],
        "created_at":"2026-10-20T11:20:18+00:00","initial_universe":pool,
        "initial_capital_per_strategy":"100000.00","first_possible_execution":"2026-10-21",
        "model_sha256":recovery._sha(native),"candidate_manifest_sha256":"a"*64,
        "strategy":{"top_k":20,"keep_rank":40,"minimum_hold_sessions":10,"max_replacements":2,"rebalance_sessions":5},
        "execution_policy":execution.ExecutionPolicy().to_dict(),"daily_attempt_limit":3,
        "implementation_sha256":{str(code):recovery._sha(code)},
        "candidate_proofs":{"native":recovery._proof(native)},
        "calendar_sha256":recovery._sha(root/"calendar.json"),"universe_sha256":recovery._sha(root/"universe.json"),
        "adapter_sha256":legacy._digest(recovery._read(root/"adapter.json"))}
    manifest["manifest_sha256"]=legacy._digest(manifest)
    account=execution.PortfolioState.initial(execution.ExecutionPolicy())
    account.cash=execution.decimal("14258.12")
    account.positions={i:execution.Position(quantity=100,bought_today=100,bought_date="2026-10-21") for i in pool[:20]}
    failed={"session":"2026-10-21","phase":"close","status":"failed","attempt":3,"error":"seam mismatch"}
    rp=save(root/"receipts/2026-10-21_close_3.json",failed)
    state={"schema_version":1,"manifest_sha256":manifest["manifest_sha256"],
        "accounts":{"model":account.to_dict()},"holding_clock":{"holding_started":{i:"2026-10-21" for i in pool[:20]},"record_origins":{},"audit":[]},
        "rebalance_anchor":"2026-10-21","attempts":{"2026-10-21_close":3},"receipts":[rp],
        "completed":{"2026-10-22_open":{"status":"blocked_observation_gap_no_fill","session":"2026-10-22","phase":"open"}},
        "signals":{},"valuations":[],"unresolved_actions":{"model":{}},
        "observation_gaps":{"2026-10-21":{"session":"2026-10-21","reason":"missing_whole_session_close_observation","detected_at":"2026-10-22T01:36:12+00:00"}}}
    state["state_sha256"]=legacy._digest(state)
    save(root/"manifest.json",manifest);save(root/"state.json",state)
    for child in root.iterdir():
        if child.is_dir():shutil.copytree(child,archived/child.name)
        else:shutil.copyfile(child,archived/child.name)
    evidence=save(tmp_path/"real-source-fixture.json",{"original":True})
    offline=save(tmp_path/"offline-audit.json",{"kind":"a15_offline_full_pool_seam_audit","status":"passed","instrument_count":1300,
        "instruments_sha256":legacy._digest(pool),"session":"2026-10-22","strict_source_validation":True,
        "seam_tolerance_relaxed":False,"source_proofs":[evidence]})
    code.write_text("new audited engineering source\n")
    tested={str(code):recovery._sha(code),str(Path(recovery.__file__).resolve()):recovery._sha(recovery.__file__)}
    tests=save(tmp_path/"test-report.json",{"kind":"a15_offline_recovery_tests","status":"passed","test_count":20,
        "network_called":False,"evidence":[evidence],"implementation_sha256":tested})
    declaration={"kind":recovery.DECLARATION_KIND,"schema_version":1,"declared_at":"2026-10-22T07:59:00+00:00", "paper_dir":str(root),
        "gap_sessions":["2026-10-21"],"resume_not_before_session":"2026-10-22", "archived_sources":{str(code):recovery._proof(source)},
        "authorized_code_updates":{str(code):{"before_sha256":manifest["implementation_sha256"][str(code)],"after_sha256":recovery._sha(code)}},
        "added_implementation_proofs":{str(Path(recovery.__file__).resolve()):recovery._sha(recovery.__file__)},"offline_audit":offline,"test_report":tests}
    decl=tmp_path/"declaration.json";save(decl,declaration)
    return {"root":root,"before":before,"code":code,"decl":decl,"declaration":declaration,"old_state":state,"old_manifest":manifest}


def migrate(case):
    return recovery.migrate_recovery(case["root"],before_directory=case["before"],declaration_path=case["decl"],now=NOW)


def test_explicit_revision_preserves_account_gap_and_original_receipts(case):
    result=migrate(case);assert result["status"]=="migrated_planned_awaiting_new_audited_close"
    state=recovery._read(case["root"]/"state.json");manifest=recovery._read(case["root"]/"manifest.json")
    for key,value in case["old_state"].items():
        if key not in {"manifest_sha256","state_sha256"}:assert state[key]==value
    for key,value in case["old_manifest"].items():
        if key not in {"manifest_sha256","implementation_sha256","candidate_proofs"}:assert manifest[key]==value
    assert state["completed"]["2026-10-22_open"]["status"]=="blocked_observation_gap_no_fill"
    assert state["attempts"]["2026-10-21_close"]==3
    recovery.verify_recovery_binding(case["root"],manifest,state)
    assert recovery.pending_gaps(state)==state["observation_gaps"]
    assert recovery.pending_gaps(state,close_session="2026-10-22")=={}
    assert recovery.pending_gaps(state,close_session="2026-10-21")==state["observation_gaps"]


def test_same_declaration_is_idempotent_without_rewriting_state(case):
    migrate(case);before={name:(case["root"]/name).read_bytes() for name in ["state.json","manifest.json"]}
    assert migrate(case)["status"]=="already_migrated"
    assert all((case["root"]/name).read_bytes()==value for name,value in before.items())


@pytest.mark.parametrize("field,value",[("instrument_count",1299),("strict_source_validation",False),("seam_tolerance_relaxed",True),("status","failed")])
def test_incomplete_or_relaxed_real_pool_audit_cannot_migrate(case,field,value):
    path=Path(case["declaration"]["offline_audit"]["path"]);audit=recovery._read(path);audit[field]=value
    case["declaration"]["offline_audit"]=save(path,audit);save(case["decl"],case["declaration"])
    with pytest.raises(ValueError,match="fixed-pool"):migrate(case)
    assert recovery._read(case["root"]/"state.json")==case["old_state"]


def test_all_old_source_bytes_and_old_receipts_must_survive(case):
    archived=Path(next(iter(case["declaration"]["archived_sources"].values()))["path"]);archived.write_text("changed")
    with pytest.raises(ValueError,match="evidence"):migrate(case)


def test_receipt_archive_cannot_be_missing(case):
    next((case["before"]/"paper/receipts").glob("*.json")).unlink()
    with pytest.raises((ValueError,FileNotFoundError)):migrate(case)


def test_account_drift_since_archive_rejected(case):
    state=recovery._read(case["root"]/"state.json");state["accounts"]["model"]["cash"]="0.00";legacy._save_state(case["root"],state)
    with pytest.raises(ValueError,match="exact frozen"):migrate(case)


def test_unauthorized_training_or_execution_module_update_rejected(case,monkeypatch):
    monkeypatch.setattr(recovery,"_UPDATABLE",set())
    with pytest.raises(ValueError,match="Training, model"):migrate(case)


def test_undeclared_engineering_changes_rejected(case):
    case["declaration"]["authorized_code_updates"]={};save(case["decl"],case["declaration"])
    with pytest.raises(ValueError,match="Every changed|exact revised"):migrate(case)


def test_future_declared_time_cannot_migrate(case):
    case["declaration"]["declared_at"]="2026-10-23T08:00:00+00:00";save(case["decl"],case["declaration"])
    with pytest.raises(ValueError,match="actual predeclared"):migrate(case)


def test_new_gap_not_covered_and_history_cannot_be_erased(case):
    migrate(case);state=recovery._read(case["root"]/"state.json")
    state["observation_gaps"]["2026-10-22"]={"session":"2026-10-22"}
    assert recovery.pending_gaps(state,close_session="2026-10-23")=={"2026-10-22":{"session":"2026-10-22"}}
    del state["observation_gaps"]["2026-10-21"]
    with pytest.raises(ValueError,match="cannot be edited"):recovery.pending_gaps(state)


def test_tampered_event_or_source_proof_fails_closed(case):
    migrate(case);state=recovery._read(case["root"]/"state.json")
    Path(state[recovery.STATE_KEY]["event"]["path"]).write_text('{}')
    with pytest.raises(ValueError,match="evidence"):recovery.pending_gaps(state)


def test_two_file_transaction_interruption_refuses_read_or_automatic_retry(case,monkeypatch):
    original=legacy._write
    def interrupted(path,value):
        if Path(path)==case["root"]/"state.json":raise OSError("synthetic power loss")
        return original(path,value)
    monkeypatch.setattr(legacy,"_write",interrupted)
    with pytest.raises(OSError):migrate(case)
    assert (case["root"]/recovery.TRANSACTION).exists()
    with pytest.raises(ValueError,match="Interrupted"):migrate(case)
    with pytest.raises(ValueError,match="Interrupted"):
        recovery.verify_recovery_binding(case["root"],recovery._read(case["root"]/"manifest.json"),recovery._read(case["root"]/"state.json"))


def close_fixture(case,monkeypatch):
    migrate(case);state=recovery._read(case["root"]/"state.json");manifest=recovery._read(case["root"]/"manifest.json")
    proof=save(case["decl"].parent/"current-rights.json",{"fixture":"all current held original guards verified"})
    monkeypatch.setattr(recovery,"_rights_collection",lambda batch,held,session:(proof,[]))
    valuation={"model":{"valuation_complete":True,"equity_lower_bound":"99000.00","equity_upper_bound":"99000.00"}}
    signal={"signal_date":"2026-10-22","execution_session":"2026-10-23","created_at":NOW.isoformat(),
        "model_sha256":manifest["model_sha256"],"allowed":{"model":True}}
    signal["signal_sha256"]=legacy._digest(signal)
    receipt={"status":"completed","phase":"close","session":"2026-10-22","signal_sha256":signal["signal_sha256"],"valuations":valuation}
    batch={"phase":"close","session":"2026-10-22","evidence":[proof],"corporate_actions":[]}
    state["signals"]["2026-10-22"]=signal
    state["valuations"].append({"session":"2026-10-22","observed_at":NOW.isoformat(),"accounts":valuation})
    return state,receipt,signal,batch


def commit_close(case,state,receipt):
    receipt["recorded_at"]=NOW.isoformat()
    rp=save(case["root"]/"receipts/2026-10-22_close_1.json",receipt)
    state["receipts"].append(rp)
    state["completed"]["2026-10-22_close"]={**receipt,"receipt_path":rp["path"],"receipt_sha256":rp["sha256"]}


def test_only_real_committed_new_complete_close_can_activate_future_open(case,monkeypatch):
    state,receipt,signal,batch=close_fixture(case,monkeypatch)
    recovery.activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=NOW)
    with pytest.raises(ValueError,match="actual committed"):recovery.pending_gaps(state)
    commit_close(case,state,receipt)
    assert recovery.pending_gaps(state)=={}
    assert "2026-10-21" in state["observation_gaps"]
    assert state["completed"]["2026-10-22_open"]["status"]=="blocked_observation_gap_no_fill"
    state["observation_gaps"]["2026-10-23"]={"session":"2026-10-23"}
    assert recovery.pending_gaps(state)=={"2026-10-23":{"session":"2026-10-23"}}


@pytest.mark.parametrize("problem",["partial","unknown_right","wrong_model","old_signal","no_rights_collection","new_gap"])
def test_incomplete_current_close_does_not_activate(case,monkeypatch,problem):
    state,receipt,signal,batch=close_fixture(case,monkeypatch)
    if problem=="partial":receipt["valuations"]["model"]["valuation_complete"]=False
    elif problem=="unknown_right":state["unresolved_actions"]["model"]={"SH600000":"unknown"}
    elif problem=="wrong_model":signal["model_sha256"]="b"*64
    elif problem=="old_signal":signal["created_at"]="2026-10-21T08:00:00+00:00"
    elif problem=="new_gap":state["observation_gaps"]["2026-10-22"]={"session":"2026-10-22"}
    else:
        def missing(*args):raise ValueError("missing rights collection")
        monkeypatch.setattr(recovery,"_rights_collection",missing)
    with pytest.raises(ValueError):recovery.activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=NOW)
    assert state[recovery.STATE_KEY]["activation"] is None


def test_missed_held_exdate_cannot_be_posted_or_ignored(case,monkeypatch):
    state,receipt,signal,batch=close_fixture(case,monkeypatch)
    proof=batch["evidence"][0]
    monkeypatch.setattr(recovery,"_rights_collection",lambda *args:(proof,[{"instrument":"SH600000","ex_date":"2026-10-21",
        "record_date":"2026-10-20","event_id":"fixture-dividend","cash_per_share":".1","bonus_ratio":"0"}]))
    with pytest.raises(ValueError,match="missed held ex-date"):
        recovery.activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=NOW)


def test_proven_zero_original_record_ownership_is_not_an_unknown_missed_entitlement(case,monkeypatch):
    state,receipt,signal,batch=close_fixture(case,monkeypatch)
    state['accounts']['model']['holdings_history']['2026-10-20']={}
    event={"instrument":"SH600000","ex_date":"2026-10-21","record_date":"2026-10-20", "event_id":"fixture-dividend","cash_per_share":".1","bonus_ratio":"0"}
    monkeypatch.setattr(recovery,"_rights_collection",lambda *args:(batch['evidence'][0],[event]))
    recovery.activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=NOW)
    assert state[recovery.STATE_KEY]['activation'] is not None
    assert not state['accounts']['model']['entitlements']


def test_activation_tamper_or_receipt_rewrite_fails_closed(case,monkeypatch):
    state,receipt,signal,batch=close_fixture(case,monkeypatch)
    recovery.activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=NOW);commit_close(case,state,receipt)
    state[recovery.STATE_KEY]["activation"]["session"]="2026-10-21"
    with pytest.raises(ValueError,match="activation checksum"):recovery.pending_gaps(state)


def test_no_recovery_metadata_keeps_original_gap_blocking():
    state={"observation_gaps":{"2026-10-21":{"reason":"missing"}}}
    assert recovery.pending_gaps(state)==state["observation_gaps"]
    recovery.verify_recovery_binding("/unused",{},state)


def dividend_batch(tmp_path):
    from stopat30m.data.ashare_actions import REQUIRED, SOURCE
    root=tmp_path/'dividends';root.mkdir()
    request={"code":"sh.600000","year":"2026","yearType":"operate"}
    payload={"generation":"synthetic-current-collection","request":request,"fetched_at":NOW.isoformat(),
        "fields":sorted(REQUIRED),"rows":[]}
    original=save(root/'SH600000_2026.json',payload)
    collection=save(root/'collection.json',{"schema_version":1,"source":SOURCE,"generation":payload['generation'],
        "instruments":["SH600000"],"years":[2026],"year_type":"operate","as_of_date":"2026-10-22",
        "responses":{"SH600000_2026":{"file":"SH600000_2026.json","sha256":original['sha256']}},
        "failures":{},"status":"complete"})
    # Exact original ValidatedBaostockDividendClient payload; no synthetic markers.
    guard=save(tmp_path/'guard.json',{"request":request,"server_echo":request,"server_error_code":"0","status":"verified",
        "complete_row_iteration_observed":True,"rows_verified":0,"started_at":NOW.isoformat(),"finished_at":NOW.isoformat()})
    return {"evidence":[original,collection,guard]},collection


def test_real_format_current_empty_dividend_response_requires_and_accepts_terminal_guard(tmp_path):
    batch,proof=dividend_batch(tmp_path)
    actual,events=recovery._rights_collection(batch,{"SH600000"},"2026-10-22")
    assert actual==proof and events==[]


def test_fully_marked_current_dividend_guard_is_also_strictly_supported(tmp_path):
    batch,proof=dividend_batch(tmp_path);path=Path(batch['evidence'][-1]['path']);guard=recovery._read(path)
    guard.update(kind='baostock_metadata_server_echo_guard',query='query_dividend_data')
    batch['evidence'][-1]=save(path,guard)
    assert recovery._rights_collection(batch,{'SH600000'},'2026-10-22')==(proof,[])


@pytest.mark.parametrize('marker',[{'kind':'incorrect','query':'query_dividend_data'},
    {'kind':'baostock_metadata_server_echo_guard','query':'query_stock_basic'},
    {'kind':'baostock_metadata_server_echo_guard'},{'query':'query_dividend_data'}])
def test_wrong_or_partial_dividend_guard_markers_cannot_be_silently_relabelled(tmp_path,marker):
    batch,_=dividend_batch(tmp_path);path=Path(batch['evidence'][-1]['path']);guard=recovery._read(path)
    guard.update(marker);batch['evidence'][-1]=save(path,guard)
    with pytest.raises(ValueError):recovery._rights_collection(batch,{'SH600000'},'2026-10-22')


@pytest.mark.parametrize('key',['request','server_echo','server_error_code','status','rows_verified',
    'started_at','finished_at','complete_row_iteration_observed'])
def test_unmarked_sdk_guard_requires_every_original_field(tmp_path,key):
    batch,_=dividend_batch(tmp_path);path=Path(batch['evidence'][-1]['path']);guard=recovery._read(path)
    del guard[key];batch['evidence'][-1]=save(path,guard)
    with pytest.raises(ValueError):recovery._rights_collection(batch,{'SH600000'},'2026-10-22')


@pytest.mark.parametrize('request_value',[{'code':'sh.600000','year':'2026','yearType':'operate','extra':'x'},
    {'code':'sh.600000','year':'2026'}, {'code':'sh.600000','yearType':'operate'},
    {'code':'sh.600001','year':'2026','yearType':'operate'},
    {'code':'sh.600000','year':2026,'yearType':'operate'},
    {'code':'sh.600000','year':'2026','yearType':'report'}])
def test_guard_request_has_exact_complete_source_identity(tmp_path,request_value):
    batch,_=dividend_batch(tmp_path);path=Path(batch['evidence'][-1]['path']);guard=recovery._read(path)
    guard['request']=guard['server_echo']=request_value;batch['evidence'][-1]=save(path,guard)
    with pytest.raises(ValueError):recovery._rights_collection(batch,{'SH600000'},'2026-10-22')


@pytest.mark.parametrize('change',[{'rows_verified':1},{'rows_verified':True},{'server_error_code':'1'},
    {'status':'pending'},{'complete_row_iteration_observed':False},
    {'started_at':'2026-10-23T08:00:00+00:00'}, {'extra_unmarked_field':'not frozen schema'}])
def test_original_guard_terminal_counts_times_and_exact_shape_remain_strict(tmp_path,change):
    batch,_=dividend_batch(tmp_path);path=Path(batch['evidence'][-1]['path']);guard=recovery._read(path)
    guard.update(change);batch['evidence'][-1]=save(path,guard)
    with pytest.raises(ValueError):recovery._rights_collection(batch,{'SH600000'},'2026-10-22')


def test_two_original_shape_guards_for_one_request_are_ambiguous_and_rejected(tmp_path):
    batch,_=dividend_batch(tmp_path);guard=recovery._read(Path(batch['evidence'][-1]['path']))
    batch['evidence'].append(save(tmp_path/'duplicate-guard.json',guard))
    with pytest.raises(ValueError,match='Exact terminal'):recovery._rights_collection(batch,{'SH600000'},'2026-10-22')


@pytest.mark.parametrize('problem',['missing_guard','unfinished_guard','wrong_held_set','stale_day','missing_original_proof'])
def test_actual_rights_guard_cannot_infer_absence_from_incomplete_or_stale_data(tmp_path,problem):
    batch,_=dividend_batch(tmp_path);held={"SH600000"};day="2026-10-22"
    if problem=='missing_guard':batch['evidence']=batch['evidence'][:2]
    elif problem=='unfinished_guard':
        proof=batch['evidence'][-1];path=Path(proof['path']);g=recovery._read(path);g['complete_row_iteration_observed']=False
        batch['evidence'][-1]=save(path,g)
    elif problem=='wrong_held_set':held.add('SH600001')
    elif problem=='stale_day':day='2026-10-23'
    else:batch['evidence']=batch['evidence'][1:]
    with pytest.raises(ValueError):recovery._rights_collection(batch,held,day)


def test_activation_does_not_accept_a_foreign_or_uncommitted_receipt(case,monkeypatch):
    state,receipt,signal,batch=close_fixture(case,monkeypatch)
    recovery.activate_after_close(state,receipt=receipt,signal=signal,batch=batch,now=NOW);commit_close(case,state,receipt)
    state['receipts']=[]
    with pytest.raises(ValueError,match='committed receipt'):recovery.pending_gaps(state)


def source_repair_fixture(case):
    folder=case['decl'].parent
    original_proof=save(folder/'original-factor-source.json',{'original':True})
    original={'kind':'low_turnover_union_close_increment','session':'2026-10-20',
        'snapshot_path':str(folder/'old-snapshot'),'snapshot_manifest_sha256':'a'*64,
        'parent_snapshot_path':str(folder/'unchanged-base19'),'daily_quotes':{'SH600000':{'close':10,'factor':.6}},
        'metadata':{'SH600000':{'is_st':False}},'current_index_instruments':case['old_manifest']['initial_universe'],
        'bridge_policy':{'price_rtol':.0005,'factor_rtol':.0005},'bound_inputs':[original_proof]}
    original['result_sha256']=legacy._digest(original)
    oldproof=save(folder/'original-refresh.json',original)
    new_manifest=save(folder/'corrected-snapshot/manifest.json',{'corrected':True})
    contract=save(folder/'factor-contract.json',{'evidenced_official_factor_repair':True})
    revised={**deepcopy(original),'snapshot_path':str(folder/'corrected-snapshot'),
        'snapshot_manifest_sha256':new_manifest['sha256'],'supersedes':oldproof,'repair_contract':contract,
        'bound_inputs':[original_proof,contract,new_manifest]}
    revised['result_sha256']=legacy._digest({k:v for k,v in revised.items() if k!='result_sha256'})
    source=save(folder/'corrected-refresh.json',revised)
    audit_path=Path(case['declaration']['offline_audit']['path']);audit=recovery._read(audit_path);audit['corrected_snapshot']=new_manifest
    case['declaration']['offline_audit']=save(audit_path,audit)
    case['declaration']['source_repair']=source
    save(case['decl'],case['declaration'])
    return revised,source


def test_corrected_parent_is_separately_versioned_without_changing_raw_or_ledger(case):
    revised,source=source_repair_fixture(case);migrate(case)
    state=recovery._read(case['root']/'state.json')
    assert recovery.source_repair(state)==source
    assert state['accounts']==case['old_state']['accounts']
    assert state['valuations']==case['old_state']['valuations']
    assert recovery._read(Path(revised['supersedes']['path']))['snapshot_path']!=revised['snapshot_path']


@pytest.mark.parametrize('problem',['quote','pool','tolerance','missing_old_proof','wrong_snapshot','same_snapshot','too_late'])
def test_source_revision_cannot_replace_raw_observation_evidence_or_relax_bridge(case,problem):
    value,proof=source_repair_fixture(case)
    if problem=='quote':value['daily_quotes']['SH600000']['close']=11
    elif problem=='pool':value['current_index_instruments']=value['current_index_instruments'][:-1]
    elif problem=='tolerance':value['bridge_policy']['price_rtol']=.1
    elif problem=='missing_old_proof':value['bound_inputs']=value['bound_inputs'][1:]
    elif problem=='wrong_snapshot':value['snapshot_manifest_sha256']='b'*64
    elif problem=='same_snapshot':value['snapshot_path']=recovery._read(Path(value['supersedes']['path']))['snapshot_path']
    else:value['session']='2026-10-22'
    value['result_sha256']=legacy._digest({k:v for k,v in value.items() if k!='result_sha256'})
    case['declaration']['source_repair']=save(Path(proof['path']),value);save(case['decl'],case['declaration'])
    with pytest.raises(ValueError):migrate(case)


def test_stale_test_log_cannot_authorize_a_new_engineering_source_version(case):
    path=Path(case['declaration']['test_report']['path']);value=recovery._read(path)
    value['implementation_sha256'][str(case['code'])]='f'*64
    case['declaration']['test_report']=save(path,value);save(case['decl'],case['declaration'])
    with pytest.raises(ValueError,match='exact revised'):migrate(case)


def raw_reference_fixture(case):
    from stopat30m.ashare_low_turnover_provider import EXECUTION_FIELDS, BASIC_FIELDS
    migrate(case);state=recovery._read(case['root']/'state.json');inst='SH600000'
    state['accounts']['model']['positions'][inst].update(last_mark='10.00',last_mark_date='2026-10-21',last_mark_basis='live_observed_quote')
    directory=case['decl'].parent/'current-source';directory.mkdir()
    generation='fresh-20261022-originals';stamp=NOW.isoformat();evidence=[];responses={}
    prior=['2026-10-21','sh.600000','10.80','11.20','10.50','11.00','10.80','100','1100','3','1','0']
    current=['2026-10-22','sh.600000','11.10','12.20','11.00','12.00','11.00','100','1200','3','1','0']
    for key in ['stock_basic',inst+'_raw',inst+'_hfq']:
        basic=key=='stock_basic'
        flag='3' if key.endswith('_raw') else '1'
        request=({'code':'','code_name':''} if basic else {'code':'sh.600000','fields':EXECUTION_FIELDS,
            'start_date':'2026-10-21','end_date':'2026-10-22','frequency':'d','adjustflag':flag})
        rows=[['sh.600000','fixture','2000-01-01','','1','1']] if basic else [deepcopy(prior),deepcopy(current)]
        if not basic:
            for row in rows:row[9]=flag
        payload={'generation':generation,'request':request,'fields':BASIC_FIELDS if basic else EXECUTION_FIELDS.split(','),
            'fetched_at':stamp,'rows':rows}
        original=save(directory/(key+'.json'),payload)
        guard={'kind':'baostock_metadata_server_echo_guard' if basic else 'baostock_daily_server_echo_guard',
            'request':request,'server_echo':request,'server_error_code':'0','status':'verified',
            'complete_row_iteration_observed':True,'rows_verified':len(rows),'started_at':stamp,'finished_at':stamp}
        if not basic:guard.update(row_mode_flags_repaired=False,row_mode_anomalies_observed=[])
        gp=save(directory/(key+'_guard.json'),guard)
        evidence.extend([original,gp])
        responses[key]={'file':key+'.json','sha256':original['sha256'],'fetched_at':stamp,'rows':len(rows),'guard':gp}
    capture={'kind':'low_turnover_guarded_daily_originals','source':'baostock','generation':generation,
        'start_date':'2026-10-21','session':'2026-10-22','instruments':[inst],
        'responses':responses,'status':'complete','failures':{}}
    evidence.append(save(directory/'capture.json',capture))
    batch={'phase':'close','session':'2026-10-22','evidence':evidence,'quotes':{inst:{
        'instrument':inst,'session':'2026-10-22','source':'baostock.daily_raw','close':12.,'preclose':11.,
        'source_response_sha256':responses[inst+'_raw']['sha256']}}}
    return state,batch,directory


def reseal_raw_reference(batch,directory,*,payload=None,guard=None,capture=None):
    raw_key='SH600000_raw';raw_path=directory/(raw_key+'.json');guard_path=directory/(raw_key+'_guard.json')
    captured=recovery._read(directory/'capture.json') if capture is None else capture
    if payload is not None:
        rp=save(raw_path,payload)
        captured['responses'][raw_key].update(sha256=rp['sha256'],rows=len(payload['rows']),fetched_at=payload['fetched_at'])
        batch['quotes']['SH600000']['source_response_sha256']=rp['sha256']
    if guard is not None:captured['responses'][raw_key]['guard']=save(guard_path,guard)
    save(directory/'capture.json',captured)
    for index,proof in enumerate(batch['evidence']):
        path=Path(proof['path'])
        if path in {raw_path,guard_path,directory/'capture.json'}:batch['evidence'][index]=recovery._proof(path)


def test_prior_raw_close_is_readonly_and_not_the_live_mark_or_an_old_NAV(case):
    state,batch,directory=raw_reference_fixture(case);before_state=deepcopy(state)
    before_files={p:p.read_bytes() for p in directory.iterdir()}
    assert recovery.previous_observed_close(state,batch,'SH600000','2026-10-21')==execution.decimal('11.00')
    assert state==before_state and state['accounts']['model']['positions']['SH600000']['last_mark']=='10.00'
    assert state['valuations']==[] and state['accounts']['model']['holdings_history']=={}
    assert all(path.read_bytes()==content for path,content in before_files.items())


@pytest.mark.parametrize('problem',['no_event','already_activated','wrong_previous','extra_gap','wrong_basis','wrong_mark_day',
    'wrong_hash','no_raw_proof','no_guard_proof','duplicate_raw_proof','duplicate_capture_proof','wrong_quote_close','wrong_quote_preclose'])
def test_prior_reference_cannot_escape_first_recovery_or_current_bound_originals(case,problem):
    state,batch,directory=raw_reference_fixture(case);previous='2026-10-21'
    if problem=='no_event':del state[recovery.STATE_KEY]
    elif problem=='already_activated':state[recovery.STATE_KEY]['activation']={}
    elif problem=='wrong_previous':previous='2026-10-20'
    elif problem=='extra_gap':state['observation_gaps']['2026-10-22']={'session':'2026-10-22'}
    elif problem=='wrong_basis':state['accounts']['model']['positions']['SH600000']['last_mark_basis']='observed_raw_close'
    elif problem=='wrong_mark_day':state['accounts']['model']['positions']['SH600000']['last_mark_date']='2026-10-20'
    elif problem=='wrong_hash':batch['quotes']['SH600000']['source_response_sha256']='a'*64
    elif problem=='no_raw_proof':batch['evidence']=[p for p in batch['evidence'] if Path(p['path']).name!='SH600000_raw.json']
    elif problem=='no_guard_proof':batch['evidence']=[p for p in batch['evidence'] if Path(p['path']).name!='SH600000_raw_guard.json']
    elif problem=='duplicate_raw_proof':batch['evidence'].append(next(p for p in batch['evidence'] if Path(p['path']).name=='SH600000_raw.json'))
    elif problem=='duplicate_capture_proof':batch['evidence'].append(batch['evidence'][-1])
    elif problem=='wrong_quote_close':batch['quotes']['SH600000']['close']=13
    else:batch['quotes']['SH600000']['preclose']=10
    with pytest.raises(ValueError):recovery.previous_observed_close(state,batch,'SH600000',previous)


@pytest.mark.parametrize('problem',['request_end','request_mode','generation','row_code','row_mode','missing_previous','missing_current',
    'duplicate_date','previous_halt','previous_zero','previous_NaN','previous_infinite','terminal','row_count','capture_partial','capture_generation'])
def test_daily_original_identity_generation_modes_dates_and_terminal_evidence_stay_strict(case,problem):
    state,batch,directory=raw_reference_fixture(case)
    payload=recovery._read(directory/'SH600000_raw.json');guard=recovery._read(directory/'SH600000_raw_guard.json')
    capture=recovery._read(directory/'capture.json')
    if problem=='request_end':payload['request']['end_date']='2026-10-21'
    elif problem=='request_mode':payload['request']['adjustflag']='1'
    elif problem=='generation':payload['generation']='old-or-foreign-generation'
    elif problem=='row_code':payload['rows'][0][1]='sh.600001'
    elif problem=='row_mode':payload['rows'][0][9]='1'
    elif problem=='missing_previous':payload['rows']=payload['rows'][1:]
    elif problem=='missing_current':payload['rows']=payload['rows'][:1]
    elif problem=='duplicate_date':payload['rows'].insert(0,deepcopy(payload['rows'][0]))
    elif problem=='previous_halt':payload['rows'][0][10]='0'
    elif problem=='previous_zero':payload['rows'][0][5]='0'
    elif problem=='previous_NaN':payload['rows'][0][5]='NaN'
    elif problem=='previous_infinite':payload['rows'][0][5]='Infinity'
    elif problem=='terminal':guard['complete_row_iteration_observed']=False
    elif problem=='row_count':guard['rows_verified']=1
    elif problem=='capture_partial':capture['status']='collecting'
    else:capture['generation']='foreign-capture-generation'
    if problem not in ['terminal','row_count']:guard['rows_verified']=len(payload['rows'])
    reseal_raw_reference(batch,directory,payload=payload,guard=guard,capture=capture)
    with pytest.raises(ValueError):recovery.previous_observed_close(state,batch,'SH600000','2026-10-21')


def test_source_previous_close_never_guesses_a_reference_from_current_preclose(case):
    state,batch,directory=raw_reference_fixture(case)
    payload=recovery._read(directory/'SH600000_raw.json');payload['rows'][1][6]='9.00'
    batch['quotes']['SH600000']['preclose']=9.
    reseal_raw_reference(batch,directory,payload=payload)
    # The original close engine must reject an unexplained 9-vs-11 reference.
    # This helper returns the real previous close; it never fits or substitutes 9.
    assert recovery.previous_observed_close(state,batch,'SH600000','2026-10-21')==execution.decimal('11.00')
