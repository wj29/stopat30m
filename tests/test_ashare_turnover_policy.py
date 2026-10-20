"""Predeclared planner semantics; synthetic holdings, no candidate returns."""
from copy import deepcopy
from decimal import Decimal as D

import pytest

from stopat30m import ashare_optimization_execution as legacy
from stopat30m.ashare_turnover_policy import build_turnover_plan, AGE_BASIS
from stopat30m.backtest import ashare_execution as ex

A, B, C, E, F, STAR = 'SH600000','SZ000001','SH600009','SH600010','SZ000002','SH688001'
# Exact session excerpt verified against the bound project exchange calendar;
# weekends and the2025 Spring Festival closure are not counted as holding days.
CAL = ['2025-01-20','2025-01-21','2025-01-22','2025-01-23','2025-01-24','2025-01-27',
 '2025-02-05','2025-02-06','2025-02-07','2025-02-10','2025-02-11','2025-02-12',
 '2025-02-13','2025-02-14','2025-02-17','2025-02-18','2025-02-19','2025-02-20',
 '2025-02-21','2025-02-24','2025-02-25','2025-02-26','2025-02-27','2025-02-28']


def account(names=(), *, cash='100000',quantity=100):
 return ex.PortfolioState(D(cash),{inst:ex.Position(quantity,last_mark=D('10'),last_mark_date=CAL[0]) for inst in names})


def plan(state, ranked=(B,C,E,F), *, session_index=10,hold=10,cap=2,origins=None,top=2,keep=None,prices=None,affordable=False):
 ranked=list(ranked);policy=legacy.RankBufferPolicy(top_k=top,keep_rank=keep or top,
  require_signal_affordability=affordable)
 kwargs=dict(signal_date=CAL[session_index-1],session=CAL[session_index],ranked_instruments=ranked,
  raw_signal_prices=prices if prices is not None else {inst:'10' for inst in ranked},
  boards={inst:'star' if inst==STAR else 'sse_main' for inst in ranked},signal_equity='100000.00',
  calendar=CAL,holding_started=origins if origins is not None else {inst:CAL[0] for inst in state.positions},
  minimum_hold_sessions=hold,max_replacements=cap,execution_policy=ex.ExecutionPolicy(),base_policy=policy)
 return build_turnover_plan(state,**kwargs),kwargs


def test_holding_age_uses_execution_calendar_index_and_buy_day_zero():
 state=account([A]);(locked,audit),_=plan(state,session_index=9,hold=10,cap=2,top=1)
 assert locked.retained==(A,) and not locked.entries and audit['holding_age_sessions'][A]==9
 (allowed,audit),_=plan(state,session_index=10,hold=10,cap=2,top=1)
 assert allowed.exits==(A,) and allowed.selected==(B,) and audit['holding_age_sessions'][A]==10
 assert audit['calendar_age_basis']==AGE_BASIS
 assert CAL[10]=='2025-02-11'  #22 calendar days, exactly10 actual sessions.
 (dayzero,audit),_=plan(state,session_index=10,origins={A:CAL[10]},hold=10,top=1)
 assert dayzero.retained==(A,) and audit['holding_age_sessions'][A]==0


def test_twenty_session_boundary_is_not_twenty_calendar_days():
 state=account([A]);(before,_),_=plan(state,session_index=19,hold=20,top=1)
 assert before.retained==(A,)
 (at,_),_=plan(state,session_index=20,hold=20,top=1)
 assert at.exits==(A,)


def test_cap_selects_missing_ranks_first_then_worst_rank_then_instrument():
 state=account([A,B,C,E]);ranked=[F,STAR,'SH600011','SH600012',C,E]
 (frozen,audit),_=plan(state,ranked,hold=0,cap=3,top=4,keep=4)
 assert audit['exit_priority_worst_first']==[A,B,E,C]
 assert frozen.exits==(A,B,E) and frozen.retained==(C,)
 assert [r[0] for r in frozen.entries]==ranked[:3]
 assert audit['cap_forced_retained']==[C]
 assert frozen.budget_per_slot_cny=='25000.00'


def test_normal_keep_rank_buffer_survives_and_does_not_reweight_incumbent():
 state=account([A],quantity=399);(frozen,audit),_=plan(state,[B,C,A,E,F],hold=0,cap=2,top=2,keep=4)
 assert frozen.retained==(A,) and not frozen.exits and frozen.selected==(A,B)
 assert state.positions[A].quantity==399 and audit['base_retained']==[A]


def test_minimum_hold_locks_out_of_scope_unscored_security_and_reserves_slot():
 state=account([A]);(frozen,audit),_=plan(state,[B,C,E],hold=20,cap=2,top=2)
 assert frozen.retained==(A,) and frozen.selected==(A,B)
 assert len(frozen.entries)==1 and audit['minimum_hold_locked']==[A]


def test_initial_build_is_not_limited_by_exit_cap():
 ranked=[f'SH{600000+i:06d}' for i in range(25)]
 (frozen,audit),_=plan(account(),ranked,hold=20,cap=2,top=20,keep=40)
 assert len(frozen.entries)==20 and not frozen.exits and audit['initial_build']
 assert frozen.budget_per_slot_cny=='5000.00'


def test_existing_vacancies_can_be_filled_in_addition_to_one_permitted_exit():
 (frozen,audit),_=plan(account([A,B]),[C,E,F,STAR,A,B],hold=0,cap=1,top=4,keep=4)
 assert len(frozen.exits)==1 and len(frozen.retained)==1 and len(frozen.entries)==3
 assert audit['entry_slots']==3 and frozen.budget_per_slot_cny=='25000.00'


def test_cap_zero_keeps_all_current_holdings_and_does_not_create_exits():
 (frozen,audit),_=plan(account([A,B]),[C,E,F],hold=0,cap=0,top=2)
 assert set(frozen.retained)=={A,B} and not frozen.exits and not frozen.entries
 assert set(audit['cap_forced_retained'])=={A,B}


def test_locked_overflow_is_retained_without_new_entries_or_reweighting():
 state=account([A,B,C,E,F]);before=state.to_dict();(frozen,audit),_=plan(state,[STAR],hold=20,cap=2,top=3)
 assert set(frozen.retained)==set(state.positions) and len(frozen.selected)==5
 assert not frozen.entries and not frozen.exits and audit['overflow_count']==2
 assert state.to_dict()==before


@pytest.mark.parametrize('affordable',[False,True])
def test_unconstrained_baseline_is_exact_original_plan_and_requires_no_new_clock_metadata(affordable):
 state=account([A,C]);(frozen,audit),kwargs=plan(state,[B,STAR,E,A,C,F],hold=0,cap=None,
  top=2,keep=4,origins={},prices={B:'10',STAR:'100000',E:'10',A:'10',C:'10',F:'10'},affordable=affordable)
 expected=legacy.build_rank_buffer_plan(state,signal_date=kwargs['signal_date'],ranked_instruments=kwargs['ranked_instruments'],
  raw_signal_prices=kwargs['raw_signal_prices'],boards=kwargs['boards'],signal_equity=kwargs['signal_equity'],
  policy=kwargs['base_policy'],execution_policy=kwargs['execution_policy'])
 assert frozen==expected and frozen.to_dict()==expected.to_dict()
 assert audit['unconstrained_baseline_exact'] and not audit['holding_clock_enforced']


def test_entry_prefix_keeps_original_fee_lot_budget_and_affordability_filter():
 state=account([A]);ranked=[STAR,B,C,E]
 (frozen,audit),kwargs=plan(state,ranked,hold=20,cap=2,top=2,keep=2,affordable=True,
  prices={STAR:'100000',B:'10',C:'100000',E:'10'})
 base=legacy.build_rank_buffer_plan(state,signal_date=kwargs['signal_date'],ranked_instruments=ranked,
  raw_signal_prices=kwargs['raw_signal_prices'],boards=kwargs['boards'],signal_equity=kwargs['signal_equity'],
  policy=kwargs['base_policy'],execution_policy=kwargs['execution_policy'])
 assert frozen.entries==base.entries[:1] and frozen.budget_per_slot_cny==base.budget_per_slot_cny
 assert frozen.filtered==((STAR,'signal_budget_cannot_cover_minimum_board_lot_and_fees'),)


def test_pending_integer_bonus_without_position_keeps_origin_and_slot():
 state=account();state.entitlements['bonus']={'kind':'bonus_shares','status':'pending','instrument':A,'quantity':'25'}
 before=state.to_dict();(frozen,audit),_=plan(state,[B,C],origins={A:CAL[0]},hold=20,cap=2,top=2)
 assert frozen.retained==(A,) and [r[0] for r in frozen.entries]==[B]
 assert audit['holding_age_sessions'][A]==10 and state.to_dict()==before


def test_fractional_claim_and_cash_receivable_do_not_occupy_a_tradeable_holding_slot():
 state=account();state.claims['fraction']={'instrument':A,'quantity':'.4'}
 state.entitlements['cash']={'kind':'cash_dividend','status':'pending','instrument':A,'net_cash':'10'}
 (frozen,audit),_=plan(state,[A,B,C],origins={},hold=20,cap=2,top=2)
 assert not frozen.retained and [r[0] for r in frozen.entries]==[A,B]
 assert audit['active_count_before_execution']==0


def test_exact_subshare_pending_bonus_is_real_engine_exposure_until_settlement():
 state=account();state.holdings_history[CAL[0]]={A:100}
 registered=ex.register_bonus_entitlement(state,event_id='synthetic_exact_bonus',instrument=A,
  record_date=CAL[0],ex_date=CAL[1],listing_date=CAL[11],ratio='.004',session=CAL[1],
  known_at=CAL[0]+'T23:59:59+08:00',evidence_sha256='a'*64,calendar=CAL,price='10').state
 assert registered.entitlements['synthetic_exact_bonus']['quantity']=='0.400'
 (frozen,audit),_=plan(registered,[B,C],origins={A:CAL[0]},hold=20,cap=2,top=2)
 assert frozen.retained==(A,) and audit['holding_age_sessions'][A]==10
 assert [row[0] for row in frozen.entries]==[B]


def test_partial_sale_or_additional_delivery_does_not_recompute_origin_from_quantity_or_mark():
 state=account([A],quantity=30);state.positions[A].last_mark_date=CAL[9]
 origin={A:CAL[0]};original=deepcopy(origin);(frozen,audit),_=plan(state,hold=10,top=1,origins=origin)
 assert frozen.exits==(A,) and audit['holding_age_sessions'][A]==10 and origin==original
 state.positions[A].quantity=230
 (again,report),_=plan(state,hold=10,top=1,origins=origin)
 assert again.exits==frozen.exits and report['holding_started']==audit['holding_started']


@pytest.mark.parametrize('origins',[{}, {A:'2025-01-28'},{A:CAL[11]},{A:'2025-01-20T00:00:00'}])
def test_unknown_non_session_or_future_holding_origins_fail_closed(origins):
 with pytest.raises(ValueError,match='first-buy|calendar or after execution|exact actual'):
  plan(account([A]),origins=origins)


@pytest.mark.parametrize('hold,cap',[(True,2),(-1,2),(10,True),(10,-1),(1.5,2),(10,2.5)])
def test_invalid_policy_parameters_fail_closed(hold,cap):
 with pytest.raises(ValueError,match='nonnegative integers'):plan(account(),hold=hold,cap=cap)


def test_calendar_gaps_duplicates_and_stale_signal_cannot_shift_age():
 state=account([A]);(_,kwargs)=plan(state)
 for calendar in [CAL+[CAL[-1]],list(reversed(CAL))]:
  with pytest.raises(ValueError,match='unique ordered'):build_turnover_plan(state,**{**kwargs,'calendar':calendar})
 with pytest.raises(ValueError,match='immediately preceding'):
  build_turnover_plan(state,**{**kwargs,'signal_date':CAL[8]})


def test_exit_cap_counts_refused_intent_and_does_not_add_a_new_execution_slot_rule():
 state=account([A]);before=state.to_dict();(frozen,audit),kwargs=plan(state,[B,A],hold=10,cap=1,top=1,keep=1)
 assert frozen.exits==(A,) and [r[0] for r in frozen.entries]==[B]
 day=kwargs['session'];known='2025-01-01T23:59:59+08:00'
 quotes={A:ex.OpeningQuote(day,D('9'),D('10'),'trading','synthetic lower-limit opening'),
         B:ex.OpeningQuote(day,D('10'),D('10'),'trading','synthetic executable opening')}
 metadata={inst:ex.SecurityInfo(inst,'sse_main','2000-01-03',False,known,'synthetic source identity') for inst in [A,B]}
 moved=legacy.execute_rank_buffer_plan(state,plan=frozen,session=day,quotes=quotes,metadata=metadata,calendar=CAL,policy=kwargs['execution_policy'])
 assert any(row['side']=='SELL' and row['instrument']==A and row['status']=='rejected' for row in moved.orders)
 assert not any(row['side']=='SELL' for row in moved.trades)
 assert {row['instrument'] for row in moved.trades if row['side']=='BUY'}=={B}
 assert set(moved.state.positions)=={A,B} and len(moved.state.positions)>frozen.policy.top_k
 assert len(audit['allowed_exits'])==1 and moved.audit['opening_replacements']==[]
 assert state.to_dict()==before
