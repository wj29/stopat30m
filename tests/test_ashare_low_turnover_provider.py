"""Offline source identity, resume, strict seams and live quote dispatch."""
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_low_turnover_provider as m
from stopat30m import ashare_forward as legacy
from stopat30m.data import ashare_live
from stopat30m.data.qlib_snapshot import prepare_qlib_snapshot, load_qlib_snapshot
from stopat30m.data.research_calendar import Calendar

DAY = "2026-10-20"
CLOCK = datetime.fromisoformat(DAY+"T18:00:00+08:00")
DAYS = pd.bdate_range(end=DAY,periods=46).strftime("%Y-%m-%d").tolist()
SMALL = [f"SH{600000+i:06d}" for i in range(21)]


def save(path, value):
    return m._save(path,value)


def seal_state(root,manifest,state):
    manifest = {k:v for k,v in manifest.items() if k!="manifest_sha256"}
    manifest["manifest_sha256"] = legacy._digest(manifest)
    state = {k:v for k,v in state.items() if k not in {"state_sha256","manifest_sha256"}}
    state["manifest_sha256"] = manifest["manifest_sha256"]
    state["state_sha256"] = legacy._digest(state)
    save(root/"manifest.json",manifest);save(root/"state.json",state)


def guard(request,rows,*,daily):
    return {"kind":"baostock_daily_server_echo_guard" if daily else "baostock_metadata_server_echo_guard",
        "request":request,"server_echo":{**request,**({"adjustflag":""} if daily else {})},
        "server_error_code":"0","status":"verified","complete_row_iteration_observed":True,
        "rows_verified":rows,"started_at":(CLOCK-timedelta(seconds=1)).isoformat(),
        "finished_at":CLOCK.isoformat(),"row_mode_flags_repaired":False,"row_mode_anomalies_observed":[]}


class Response:
    error_code = "0"
    def __init__(self,fields,rows,request,callback,daily):
        self.fields,self.rows,self.request,self.callback,self.daily=fields,rows,request,callback,daily
        self.i=-1
    def next(self):
        self.i+=1
        if self.i<len(self.rows):
            return True
        self.callback(guard(self.request,len(self.rows),daily=self.daily))
        return False
    def get_row_data(self):
        return self.rows[self.i]


class Source:
    def __init__(self,callback,targets=SMALL,defect=None):
        self.callback,self.targets,self.defect=callback,targets,defect
        self.calls=[]
    def login(self):
        return SimpleNamespace(error_code="0")
    def logout(self):
        return None
    def query_stock_basic(self,**request):
        self.calls.append(("basic",request))
        rows=[[s[:2].lower()+"."+s[2:],"synthetic","1999-01-01","","1","1"] for s in self.targets]
        return Response(m.BASIC_FIELDS,rows,request,self.callback,False)
    def query_history_k_data_plus(self,**request):
        self.calls.append(("daily",request))
        if self.defect=="transport" and request["adjustflag"]=="1":
            raise TimeoutError("synthetic transport interruption")
        factor=2 if request["adjustflag"]=="1" else 1
        rows=[]
        symbol=request["code"].replace(".","").upper()
        for day in DAYS:
            if not request["start_date"]<=day<=request["end_date"]:
                continue
            if day==DAY and (self.defect=="all_stale" or self.defect=="missing" and symbol==self.targets[-1]):
                continue
            status="0" if self.defect=="halt" and day==DAY and symbol==self.targets[-1] else "1"
            price=12 if self.defect=="seam" and symbol==self.targets[0] and day==DAYS[15] else 10
            flag="3" if self.defect=="mixed_mode" and factor==2 else request["adjustflag"]
            rows.append([day,request["code"],str(price*factor),str((price+1)*factor),str((price-1)*factor),
                str(price*factor),str(9.9*factor),"1000",str(price*1000),flag,status,"0"])
        return Response(m.EXECUTION_FIELDS.split(","),rows,request,self.callback,True)


@pytest.fixture
def case(tmp_path,monkeypatch):
    monkeypatch.setattr(m,"_now",lambda:CLOCK)
    pool=[f"SH{600000+i:06d}" for i in range(1300)]
    evidence=save(tmp_path/"official-source.json",{"synthetic":True,"date":"2026-10-19"})
    universe={"kind":m.WATCHLIST_KIND,"as_of":"2026-10-19","source_date":"2026-10-19",
        "instruments":pool,"bound_inputs":[evidence],"evidence":[evidence]}
    paper=tmp_path/"paper";paper.mkdir()
    universe_path=paper/"universe.json";save(universe_path,universe)
    basic=pd.DataFrame([[s[:2].lower()+"."+s[2:],"1","1999-01-01",""] for s in pool],
        columns=["code","type","ipoDate","outDate"])
    basic_path=tmp_path/"stock_basic.csv";basic.to_csv(basic_path,index=False)
    rows=[{"date":day,"is_open":day in DAYS} for day in m.natural_days(DAYS[0],DAY)]
    calendar=Calendar.from_rows(rows,DAYS[0],DAY,source="synthetic_explicit_flags",source_url="fixture://calendar")
    calendar_path=paper/"calendar.json";calendar.save(calendar_path)
    archive=tmp_path/"fixture.zip"
    with zipfile.ZipFile(archive,"w") as stream:
        stream.writestr("qlib/calendars/day.txt","\n".join(DAYS[:-1])+"\n")
        stream.writestr("qlib/calendars/day_future.txt","\n".join(DAYS)+"\n")
        stream.writestr("qlib/instruments/all.txt","".join(s+"\t"+DAYS[0]+"\t"+DAYS[-2]+"\n" for s in SMALL))
        for symbol in SMALL:
            for field,value in {"open":1.,"high":1.1,"low":.9,"close":1.,"factor":.1,"volume":100.,"vwap":1.}.items():
                stream.writestr("qlib/features/"+symbol.lower()+"/"+field+".day.bin",
                    np.asarray([0,*([value]*45)],dtype="<f4").tobytes())
    parent=prepare_qlib_snapshot(archive,tmp_path/"versions",source={"kind":"community","url":"fixture:seed"},
        expected_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    adapter={"initial_snapshot_path":str(parent.path),"calendar_path":str(calendar_path),
        "universe_path":str(universe_path),"provider_output_dir":str(tmp_path/"provider"),"stock_basic_path":str(basic_path),
        "stock_basic_sha256":m._proof(basic_path)["sha256"],"initial_snapshot_manifest_sha256":parent.manifest_sha256}
    save(paper/"adapter.json",adapter)
    manifest={"kind":m.KIND,"created_at":"2026-10-20T16:00:00+08:00","adapter_sha256":legacy._digest(adapter),
        "calendar_sha256":m._proof(calendar_path)["sha256"],"universe_sha256":m._proof(universe_path)["sha256"]}
    state={"accounts":{"model":{"positions":{},"claims":{},"entitlements":{}}}}
    seal_state(paper,manifest,state)
    provider=m.Provider.from_paper_dir(paper)
    sources=[]
    def factory(callback):
        source=Source(callback);sources.append(source);return source
    monkeypatch.setattr(m,"_client",factory)
    return SimpleNamespace(tmp=tmp_path,paper=paper,provider=provider,parent=parent,calendar=calendar,
        pool=pool,evidence=evidence,sources=sources)


def small(case,monkeypatch):
    monkeypatch.setattr(case.provider,"_universe",lambda:(SMALL,[case.evidence]))


def refresh(case):
    return case.provider._refresh_close(case.parent.path,DAY,SMALL,case.calendar)


def test_frozen_union_requires_all_1300_names_and_bound_official_source(case):
    symbols,proofs=case.provider._universe()
    assert symbols==case.pool and len(symbols)==1300
    assert any(p["sha256"]==case.evidence["sha256"] for p in proofs)
    Path(case.evidence["path"]).write_text("tampered official source")
    with pytest.raises(ValueError,match="evidence bytes"):
        case.provider._universe()


def test_new_state_rejects_old_kind_and_changed_adapter_before_network(case):
    manifest=json.loads((case.paper/"manifest.json").read_bytes())
    state=json.loads((case.paper/"state.json").read_bytes())
    assert case.provider._state()[1]["accounts"].keys()=={"model"}
    manifest["kind"]="isolated_ashare_integer_forward_paper"
    seal_state(case.paper,manifest,state)
    with pytest.raises(ValueError,match="frozen binding"):
        case.provider._state()
    assert not case.sources


def test_outside_source_clock_fails_before_any_network(case,monkeypatch):
    monkeypatch.setattr(m,"_now",lambda:datetime.fromisoformat(DAY+"T14:55:00+08:00"))
    with pytest.raises(ValueError,match="complete close"):
        case.provider.fetch_close(DAY,[],CLOCK)
    with pytest.raises(ValueError,match="opening window"):
        case.provider.fetch_open(DAY,[],CLOCK)
    monkeypatch.setattr(m,"_now",lambda:CLOCK+timedelta(days=1))
    with pytest.raises(ValueError,match="today"):
        case.provider.fetch_close(DAY,[],CLOCK)
    assert not case.sources


def test_guarded_capture_exact_requests_modes_and_complete_cache_resume(case,monkeypatch):
    root=case.tmp/"capture"
    receipt=m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:1])
    assert receipt["status"]=="complete" and set(receipt["responses"])=={"stock_basic",SMALL[0]+"_raw",SMALL[0]+"_hfq"}
    source=case.sources[-1]
    assert [r[1]["adjustflag"] for r in source.calls if r[0]=="daily"]==["3","1"]
    assert all(r[1]["fields"]==m.EXECUTION_FIELDS for r in source.calls if r[0]=="daily")
    monkeypatch.setattr(m,"_client",lambda _:pytest.fail("a complete cached capture must stay offline"))
    assert m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:1])==receipt
    guard_path=Path(receipt["responses"][SMALL[0]+"_raw"]["guard"]["path"])
    guard_path.write_text("changed")
    with pytest.raises(ValueError,match="evidence bytes"):
        m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:1])


def test_first_transport_failure_keeps_successes_and_resume_does_not_repeat_them(case,monkeypatch):
    root=case.tmp/"capture"
    first=Source(None,defect="transport")
    def factory(callback):
        first.callback=callback;return first
    monkeypatch.setattr(m,"_client",factory)
    with pytest.raises(TimeoutError):
        m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:1])
    partial=json.loads((root/"capture.json").read_bytes())
    assert partial["status"]=="partial_not_publishable" and len(partial["responses"])==2
    before={r["file"]:(root/r["file"]).read_bytes() for r in partial["responses"].values()}
    second=Source(None)
    def fresh(callback):
        second.callback=callback;return second
    monkeypatch.setattr(m,"_client",fresh)
    complete=m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:1])
    assert complete["status"]=="complete" and complete["generation"]==partial["generation"]
    assert len(second.calls)==1 and second.calls[0][1]["adjustflag"]=="1"
    assert all((root/name).read_bytes()==raw for name,raw in before.items())


def test_guard_validation_rejects_wrong_echo_empty_terminal_and_future_clock(tmp_path):
    payload={"request":{"code":"sh.600000","year":"2026","yearType":"operate"},"rows":[],"fetched_at":CLOCK.isoformat()}
    good=guard(payload["request"],0,daily=False)
    for change in ({"server_echo":{**payload["request"],"year":"2025"}},
            {"complete_row_iteration_observed":False},{"rows_verified":1},
            {"finished_at":(CLOCK+timedelta(seconds=1)).isoformat()}):
        proof=save(tmp_path/(str(len(list(tmp_path.iterdir())))+".json"),{**good,**change})
        with pytest.raises(ValueError):
            m._validate_guard(proof,payload,daily=False)


def test_mixed_hfq_mode_never_relabelled_or_given_a_factor_fallback(case,monkeypatch):
    monkeypatch.setattr(m,"_client",lambda cb:Source(cb,defect="mixed_mode"))
    with pytest.raises(ValueError,match="mode/status"):
        m.capture_daily_originals(case.tmp/"capture",DAYS[5],DAY,SMALL[:1])
    original=json.loads((case.tmp/"capture"/(SMALL[0]+"_hfq.json")).read_bytes())
    assert original["rows"][0][9]=="3"


def test_strict_real_publication_preserves_parent_and_uses_100_share_units(case,monkeypatch):
    small(case,monkeypatch)
    before={str(p.relative_to(case.parent.dataset_dir)):p.read_bytes() for p in case.parent.dataset_dir.rglob("*") if p.is_file()}
    result,path=refresh(case)
    assert result["signal_ready"] and result["bridge_policy"]["base_raw_volume_multiplier"]==100
    assert result["daily_quotes"][SMALL[0]]["close"]==10
    assert result["daily_quotes"][SMALL[0]]["preclose"]==9.9
    assert result["daily_quotes"][SMALL[0]]["factor"]==pytest.approx(.1)
    child=load_qlib_snapshot(result["snapshot_path"])
    assert child.path!=case.parent.path
    assert m.read_qlib_security(child.dataset_dir,SMALL[0]).at[DAY,"volume"]==pytest.approx(100)
    assert all((case.parent.dataset_dir/name).read_bytes()==raw for name,raw in before.items())
    calls=len(case.sources)
    assert refresh(case)[0]==result and len(case.sources)==calls
    assert case.provider._load_result(path)==result
    assert result["universe_scope"].endswith("not_daily_current_membership")


@pytest.mark.parametrize("defect,expected",[("missing","unknown"),("halt","suspended")])
def test_absence_and_actual_halt_are_distinct_and_do_not_use_old_price(case,monkeypatch,defect,expected):
    small(case,monkeypatch)
    monkeypatch.setattr(m,"_client",lambda cb:Source(cb,defect=defect))
    result,_=refresh(case)
    quote=result["daily_quotes"][SMALL[-1]]
    assert quote["tradestatus"]==expected and quote["close"] is None and quote["factor"] is None
    assert quote["bar_complete"] is False and SMALL[-1] in result["excluded"]
    child=load_qlib_snapshot(result["snapshot_path"])
    assert not np.isfinite(m.read_qlib_security(child.dataset_dir,SMALL[-1]).at[DAY,"close"])


@pytest.mark.parametrize("defect",["seam","all_stale"])
def test_bad_seam_or_stale_source_prevents_result_publication(case,monkeypatch,defect):
    small(case,monkeypatch)
    monkeypatch.setattr(m,"_client",lambda cb:Source(cb,defect=defect))
    with pytest.raises(ValueError):
        refresh(case)
    assert not (case.provider.output_dir/"daily"/DAY/"refresh_result.json").exists()
    assert not (case.provider.output_dir/"daily"/DAY/"versions").exists()


def test_published_result_rechecks_original_guard_not_only_snapshot(case,monkeypatch):
    small(case,monkeypatch)
    result,path=refresh(case)
    proof=next(p for p in result["bound_inputs"] if "/guards/" in p["path"])
    Path(proof["path"]).write_text("modified source")
    with pytest.raises(ValueError,match="evidence bytes"):
        case.provider._load_result(path)


def test_close_adds_held_claim_targets_without_claiming_extra_index_members(case,monkeypatch):
    manifest,state=case.provider._state()
    extra="SZ000001"
    state["accounts"]["model"]["claims"]={"pending":{"instrument":extra}}
    seal_state(case.paper,manifest,state)
    result={"session":DAY,"snapshot_path":"fixture-snapshot","daily_quotes":{extra:{"bar_complete":False}},
        "metadata":{},"excluded":{extra:"source_missing_session"},"bound_inputs":[]}
    calls=[]
    monkeypatch.setattr(case.provider,"_latest_refresh",lambda *_:(dict(result,session="2026-10-19"),None))
    def refresh(parent,session,targets,calendar):
        calls.append(targets);return result,None
    monkeypatch.setattr(case.provider,"_refresh_close",refresh)
    monkeypatch.setattr(case.provider,"_refresh_proofs",lambda *_:([],[case.evidence]))
    monkeypatch.setattr(case.provider,"_dividends",lambda *_:([],[]))
    batch=case.provider.fetch_close(DAY,[],CLOCK)
    assert extra in calls[0] and len(calls[0])==1301
    assert batch["instruments"]==case.pool and extra not in batch["instruments"]
    assert batch["quotes"][extra]["bar_complete"] is False


def test_live_open_uses_new_state_and_actual_receive_clock_with_current_status(case,monkeypatch):
    clock=CLOCK.replace(hour=9,minute=35)
    monkeypatch.setattr(m,"_now",lambda:clock)
    initial,_=case.provider._latest_refresh(DAY,before=True)
    monkeypatch.setattr(case.provider,"_latest_refresh",lambda *a,**k:(initial,None))
    monkeypatch.setattr(case.provider,"_dividends",lambda *_:([],[]))
    calls=[]
    def live(instruments,output,**options):
        calls.append((instruments,options))
        proof=save(output/"response.txt",{"synthetic":True})
        receipt={"source":"tencent.public_level1_observation","rejected":{},"responses":[{
            "file":"response.txt","sha256":proof["sha256"],"url":"https://qt.gtimg.cn/q=sh600000",
            "instruments":instruments,"fetched_at":clock.isoformat()}],"quotes":{SMALL[0]:{
            "instrument":SMALL[0],"session":DAY,"open":"10","preclose":"9.9","execution_price":"10.2",
            "asof":clock.isoformat(),"tradestatus":"1","is_st":True,"live_execution_eligible":True,
            "execution_price_basis":"live_observed_quote"}}}
        save(output/"receipt.json",receipt);return receipt
    monkeypatch.setattr(ashare_live,"fetch_tencent_execution_quotes",live)
    batch=case.provider.fetch_open(DAY,[SMALL[0]],clock)
    assert calls[0][1]["now"] is None and calls[0][1]["require_live"] is True
    assert batch["quotes"][SMALL[0]]["execution_price"]=="10.2"
    assert batch["metadata"][SMALL[0]]["is_st"] is True
    assert batch["metadata"][SMALL[0]]["status_session"]==DAY
    assert batch["guaranteed_fill"] is False


def test_initial_identity_and_snapshot_manifest_are_frozen_inputs(case):
    case.provider.stock_basic_path.write_text("code,type,ipoDate,outDate\nsh.600000,1,1999-01-01,\n")
    with pytest.raises(ValueError,match="frozen binding"):
        case.provider._state()
    assert not case.sources


def test_resume_cannot_expand_targets_or_change_window(case):
    root=case.tmp/"capture"
    m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:1])
    with pytest.raises(ValueError,match="exact capture"):
        m.capture_daily_originals(root,DAYS[5],DAY,SMALL[:2])
    with pytest.raises(ValueError,match="exact capture"):
        m.capture_daily_originals(root,DAYS[4],DAY,SMALL[:1])


def test_dividend_capture_uses_held_and_pending_rights_and_binds_even_empty_guard(case,monkeypatch):
    from stopat30m.data import ashare_actions as actions
    state={"accounts":{"model":{"positions":{},"claims":{"cash":{"instrument":SMALL[0]}},"entitlements":{}}}}
    captured=[]
    original_client=m._client
    def client(callback):
        result=original_client(callback)
        captured.append(result)
        return result
    monkeypatch.setattr(m,"_client",client)
    def collect(output,instruments,years,**options):
        assert instruments==[SMALL[0]] and years==[2026]
        source=options["client"]
        request={"code":"sh.600000","year":"2026","yearType":"operate"}
        source.callback(guard(request,0,daily=False))
        payload={"request":request,"fields":sorted(actions.REQUIRED),"rows":[],"generation":"synthetic",
            "fetched_at":CLOCK.isoformat()}
        proof=save(output/(SMALL[0]+"_2026.json"),payload)
        save(output/"collection.json",{"source":actions.SOURCE,"generation":"synthetic","as_of_date":DAY,
            "instruments":instruments,"years":years,"year_type":"operate","status":"complete","failures":{},
            "responses":{SMALL[0]+"_2026":{"file":Path(proof["path"]).name,"sha256":proof["sha256"]}}})
        return output
    monkeypatch.setattr(actions,"collect_dividend_evidence",collect)
    events,proofs=case.provider._dividends(DAY,"close",{"created_at":CLOCK.isoformat()},state,case.tmp/"actions")
    assert events==[] and any("dividend_guards" in p["path"] for p in proofs)
    assert len(captured)==1


def test_nonempty_dividend_rows_cannot_evade_guarded_implemented_year(case,monkeypatch):
    from stopat30m.data import ashare_actions as actions
    state={"accounts":{"model":{"positions":{SMALL[0]:{}},"claims":{},"entitlements":{}}}}
    request={"code":"sh.600000","year":"2026","yearType":"operate"}
    fields=sorted(actions.REQUIRED)
    row={field:"" for field in fields}
    row.update(code="sh.600000",dividOperateDate="2025-10-20")
    def collect(output,instruments,years,**options):
        options["client"].callback(guard(request,1,daily=False))
        payload={"request":request,"fields":fields,"rows":[[row[f] for f in fields]],
            "generation":"synthetic","fetched_at":CLOCK.isoformat()}
        proof=save(output/(SMALL[0]+"_2026.json"),payload)
        save(output/"collection.json",{"source":actions.SOURCE,"generation":"synthetic","as_of_date":DAY,
            "instruments":instruments,"years":years,"year_type":"operate","status":"complete","failures":{},
            "responses":{SMALL[0]+"_2026":{"file":Path(proof["path"]).name,"sha256":proof["sha256"]}}})
        return output
    monkeypatch.setattr(actions,"collect_dividend_evidence",collect)
    with pytest.raises(ValueError,match="implemented year"):
        case.provider._dividends(DAY,"close",{"created_at":CLOCK.isoformat()},state,case.tmp/"actions")


def test_live_extra_right_keeps_identity_outside_prior_batch_metadata(case,monkeypatch):
    clock=CLOCK.replace(hour=9,minute=35)
    monkeypatch.setattr(m,"_now",lambda:clock)
    inst=case.pool[-1]
    initial={"snapshot_path":str(case.parent.path),"session":"2026-10-19","metadata":{},"bound_inputs":[]}
    monkeypatch.setattr(case.provider,"_latest_refresh",lambda *a,**k:(initial,None))
    monkeypatch.setattr(case.provider,"_dividends",lambda *_:([],[]))
    def live(instruments,output,**options):
        proof=save(output/"response.txt",{"synthetic":True})
        value={"source":"tencent.public_level1_observation","rejected":{},"responses":[{
            "file":"response.txt","sha256":proof["sha256"],"url":"https://qt.gtimg.cn/q=sh601299",
            "instruments":instruments,"fetched_at":clock.isoformat()}],"quotes":{inst:{
            "instrument":inst,"session":DAY,"open":"10","preclose":"9.9","execution_price":"10.2",
            "asof":clock.isoformat(),"tradestatus":"1","is_st":False,"live_execution_eligible":True}}}
        save(output/"receipt.json",value);return value
    monkeypatch.setattr(ashare_live,"fetch_tencent_execution_quotes",live)
    batch=case.provider.fetch_open(DAY,[inst],clock)
    assert batch["metadata"][inst]["source"]=="frozen_registry_identity+tencent.current_quote_status"
    assert batch["metadata"][inst]["is_st"] is False
    assert any(p["path"]==str(case.provider.stock_basic_path) for p in batch["evidence"])


def test_cash_only_initial_account_does_not_make_unnecessary_dividend_queries(case,monkeypatch):
    monkeypatch.setattr(m,"_client",lambda _:pytest.fail("no held/rights targets to query"))
    manifest,state=case.provider._state()
    assert case.provider._dividends(DAY,"close",manifest,state,case.tmp/"actions")==([],[])
