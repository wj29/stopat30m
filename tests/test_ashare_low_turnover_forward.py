from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from stopat30m import ashare_low_turnover_forward as runner
from stopat30m import ashare_forward as legacy
from stopat30m.backtest import ashare_execution as ex
from stopat30m.data.research_calendar import Calendar


def clock(day, value="09:35"):
    return datetime.fromisoformat(day+"T"+value+":00+08:00")


@pytest.fixture
def paper(tmp_path,monkeypatch):
    import pandas as pd
    days=pd.date_range("2026-09-01","2026-11-10").strftime("%Y-%m-%d")
    actual=set(pd.bdate_range(days[0],days[-1]).strftime("%Y-%m-%d"))
    calendar=Calendar.from_rows([{"date":d,"is_open":d in actual} for d in days],days[0],days[-1],
        source="synthetic_fixture",source_url="fixture://explicit-calendar")
    calendar.save(tmp_path/"calendar.json")
    proof=tmp_path/"original.json"; proof.write_text('{}')
    source={"path":str(proof),"sha256":legacy._sha(proof)}
    pool=[f"SH{600000+n:06d}" for n in range(1300)]
    legacy._write(tmp_path/"universe.json",{"source_date":"2026-10-19","instruments":pool,"evidence":[source]})
    candidate=SimpleNamespace(model_sha256='a'*64,manifest={"manifest_sha256":'b'*64},
        protocol={"rebalance_sessions":5,"original_fitted_at":"2026-10-20T07:00:00+00:00"},proofs=lambda:{"fixture":source})
    monkeypatch.setattr(runner,"_candidate",lambda m:candidate)
    monkeypatch.setattr(runner,"implementation_proofs",lambda:{})
    monkeypatch.setattr(legacy,"_signal_bar_eligibility",lambda c,b,inst,s:(inst,{},'c'*64))
    monkeypatch.setattr(legacy,"_candidate_targets",lambda c,p,s,eligible:(
        {"model":{i:'.05' for i in eligible[:20]}},{"model_scores":{i:float(len(eligible)-n) for n,i in enumerate(eligible)}}))
    root=tmp_path/"paper"
    runner.initialize(root,models_directory=tmp_path,protocol_path=proof,market_audit_path=proof,
        calendar_path=tmp_path/"calendar.json",universe_path=tmp_path/"universe.json",
        adapter_config={},now=clock("2026-10-20","16:00"))
    return root,pool[:80],source


def batch(paper,day,phase):
    root,names,source=paper; stamp=clock(day,"16:30" if phase=="close" else "09:35").isoformat()
    quotes={i:{"session":day,"source":"tencent.public_level1_observation" if phase=="open" else "fixture.daily_raw",
        "fetched_at":stamp,"asof":stamp,"open":"10","close":"10","preclose":"10","factor":"1",
        "tradestatus":"trading","bar_complete":True,"execution_price":"10","execution_price_basis":"live_observed_quote"} for i in names}
    info={i:{"instrument":i,"board":"sse_main","listing_date":"2010-01-01","is_st":False,
        "status_session":day,"source":"fixture.current_identity","known_at":stamp,"delisting_period":False} for i in names}
    return {"phase":phase,"session":day,"source":"tencent.public_level1_observation" if phase=="open" else "fixture.daily_raw",
        "fetched_at":stamp,"quotes":quotes,"metadata":info,"instruments":names,
        "universe_evidence":[source],"evidence":[source],"corporate_actions":[],"snapshot_path":"fixture"}


class Provider:
    def __init__(self,paper): self.paper=paper; self.calls=0
    def fetch_close(self,day,*args): self.calls+=1; return batch(self.paper,day,"close")
    def fetch_open(self,day,*args): self.calls+=1; return batch(self.paper,day,"open")


def test_future_close_open_uses_rank_engine_actual_fill_clock_and_idempotence(paper):
    root,_,_=paper; provider=Provider(paper)
    closed=runner.run_low_turnover(root,phase="close",provider=provider,now=clock("2026-10-20","16:30"))
    assert closed["status"]=="completed" and closed["rebalance_anchor"]=="2026-10-21"
    result=runner.run_low_turnover(root,phase="open",provider=provider,now=clock("2026-10-21"))
    assert result["status"]=="completed",result
    account=result["accounts"]["model"]
    assert account["trades"] and all(t['live_order'] is False and t['commission']=='5.00' for t in account['trades'])
    assert account["turnover_audit"]["minimum_hold_sessions"]==10
    assert account["turnover_audit"]["max_replacements"]==2
    state=runner.low_turnover_status(root)["state"]
    assert set(state['holding_clock']['holding_started'].values())=={'2026-10-21'}
    before=(root/'state.json').read_bytes(); count=provider.calls
    again=runner.run_low_turnover(root,phase="open",provider=provider,now=clock("2026-10-21","09:40"))
    assert again['replayed'] and provider.calls==count and (root/'state.json').read_bytes()==before


def test_missed_open_never_fetches_or_fills(paper):
    root,_,_=paper; provider=Provider(paper)
    result=runner.run_low_turnover(root,phase='open',provider=provider,now=clock('2026-10-21','10:00'))
    assert result['status']=='expired_no_fill' and provider.calls==0
    assert runner.low_turnover_status(root)['state']['accounts']['model']['cash']=='100000.00'


def test_source_failure_preserves_cash_and_bounds_retries(paper):
    root,_,_=paper
    class Failed:
        def fetch_close(self,*args): raise RuntimeError('source unavailable')
    for n in range(3):
        result=runner.run_low_turnover(root,phase='close',provider=Failed(),now=clock('2026-10-20','16:30'))
        assert result['status']=='failed'
    result=runner.run_low_turnover(root,phase='close',provider=Failed(),now=clock('2026-10-20','17:30'))
    assert result['status']=='attempt_limit' and not result['network_called']
    state=runner.low_turnover_status(root)['state']
    assert state['valuations']==[] and state['accounts']['model']['cash']=='100000.00'


def test_bootstrap_without_prior_observations_does_not_fabricate_missing_cash_navs(paper):
    root,_,_=paper; provider=Provider(paper)
    result=runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-22','16:30'))
    assert result['status']=='completed' and result['rebalance_anchor']=='2026-10-23'
    state=runner.low_turnover_status(root)['state']
    assert not state['observation_gaps'] and len(state['valuations'])==1


def test_missing_observed_close_freezes_future_orders(paper):
    root,_,_=paper; provider=Provider(paper)
    runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-20','16:30'))
    result=runner.run_low_turnover(root,phase='open',provider=provider,now=clock('2026-10-22'))
    assert result['status']=='blocked_observation_gap_no_fill' and provider.calls==1


def test_unknown_noncash_distribution_is_blocked_before_ledger_change():
    state={'accounts':{'model':ex.PortfolioState.initial(ex.ExecutionPolicy()).to_dict()}}
    position=ex.Position(quantity=100,bought_today=0,bought_date='2026-10-20')
    account=ex.PortfolioState.from_dict(state['accounts']['model']); account.positions['SH600000']=position
    state['accounts']['model']=account.to_dict()
    with pytest.raises(ValueError,match='separate verified'):
        runner._guard_actions(state,{'corporate_actions':[{'instrument':'SH600000','bonus_ratio':'.1'}]})


def test_live_capture_includes_affordable_entries_below_rank40(paper):
    root,names,_=paper
    class Affordable(Provider):
        requested=None
        def fetch_close(self,day,*args):
            value=super().fetch_close(day,*args)
            for inst in names[:40]: value['quotes'][inst]['close']='100000'
            return value
        def fetch_open(self,day,needed,*args):
            self.requested=needed
            return super().fetch_open(day,needed,*args)
    provider=Affordable(paper)
    runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-20','16:30'))
    result=runner.run_low_turnover(root,phase='open',provider=provider,now=clock('2026-10-21'))
    assert set(provider.requested)==set(names[40:60])
    assert result['accounts']['model']['trades']


def test_real_fill_origins_enforce_D5_lock_and_D10_two_exit_cap(paper):
    root,names,_=paper; provider=Provider(paper)
    runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-20','16:30'))
    runner.run_low_turnover(root,phase='open',provider=provider,now=clock('2026-10-21'))
    _,manifest,state,_,calendar=runner._read_paper(root)
    account=ex.PortfolioState.from_dict(state['accounts']['model'])
    days=calendar.sessions(calendar.coverage_start,calendar.coverage_end)
    tracker=runner._tracker(state,days)
    for offset,count in [(5,0),(10,2)]:
        day=days[days.index('2026-10-21')+offset]
        signal={'signal_date':calendar.previous_session(day),'ranked_instruments':list(reversed(names)),
            'raw_signal_prices':{i:'10' for i in names},'boards':{i:'sse_main' for i in names},'signal_equity':'100000'}
        plan,audit=runner._plan(account,signal,days,tracker,ex.ExecutionPolicy(),day)
        assert len(plan.exits)==count
        assert set(audit['holding_age_sessions'].values())=={offset}
        if offset==5: assert not plan.entries


def test_stale_quote_does_not_commit_a_fill(paper):
    root,_,_=paper
    class Stale(Provider):
        def fetch_open(self,day,*args):
            value=super().fetch_open(day,*args)
            for row in value['quotes'].values(): row['asof']=clock(day,'09:30').isoformat()
            return value
    provider=Stale(paper)
    runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-20','16:30'))
    result=runner.run_low_turnover(root,phase='open',provider=provider,now=clock('2026-10-21'))
    assert result['status']=='failed'
    assert runner.low_turnover_status(root)['state']['accounts']['model']['cash']=='100000.00'


def test_past_bonus_without_record_date_ownership_does_not_block_new_holding():
    account=ex.PortfolioState.initial(ex.ExecutionPolicy())
    account.positions['SH600000']=ex.Position(quantity=100,bought_date='2026-10-25')
    account.holdings_history['2026-10-20']={}
    event={'event_id':'old','instrument':'SH600000','record_date':'2026-10-20','bonus_ratio':'.1'}
    state={'accounts':{'model':account.to_dict()},'corporate_actions':{'old':event}}
    runner._guard_actions(state,{'corporate_actions':[]})
    account.holdings_history={}
    state['accounts']['model']=account.to_dict()
    with pytest.raises(ValueError,match='separate verified'):
        runner._guard_actions(state,{'corporate_actions':[]})


def test_unowned_bonus_is_not_sent_into_old_tax_action_book(paper):
    root,names,_=paper
    class PriorBonus(Provider):
        def fetch_close(self,day,*args):
            value=super().fetch_close(day,*args)
            if day=='2026-10-22':
                value['corporate_actions']=[{'event_id':'unowned_stock_bonus','instrument':names[0],
                    'record_date':'2026-10-20','ex_date':'2026-10-21','bonus_ratio':'.1',
                    'stock_dividend_ratio':'.1','cash_per_share':'.1'}]
            return value
    provider=PriorBonus(paper)
    runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-20','16:30'))
    runner.run_low_turnover(root,phase='open',provider=provider,now=clock('2026-10-21'))
    runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-21','16:30'))
    result=runner.run_low_turnover(root,phase='close',provider=provider,now=clock('2026-10-22','16:30'))
    assert result['status']=='completed',result
    assert result['ignored_no_entitlement_actions'][0]['event_id']=='unowned_stock_bonus'
    state=runner.low_turnover_status(root)['state']
    assert not state['accounts']['model']['entitlements']
    original=json.loads((root/'observations/2026-10-22_close_1.json').read_text())
    assert original['corporate_actions'][0]['bonus_ratio']=='.1'
