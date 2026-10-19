"""Bind source status, actual rights and executable share ledgers together."""
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_execution_experiment as a2


def test_raw_status_is_explicitly_mapped_before_integer_execution():
    raw = pd.DataFrame([dict(date="2025-01-06", instrument="SH600000", source_complete_quote=True,
        open="10", close="10", preclose="10", tradestatus="1", isST="0")])
    basic = pd.DataFrame([dict(code="sh.600000", ipoDate="1999-11-10")])
    quotes, metadata, close = a2._daily_inputs(raw, basic, "2025-01-06", ["SH600000"])
    policy = a2.ex.ExecutionPolicy(initial_cash=10000)
    result = a2.ex.execute_target_weights(a2.ex.PortfolioState.initial(policy), session="2025-01-06",
        signal_date="2025-01-03", targets={"SH600000":Decimal(1)}, quotes=quotes, metadata=metadata,
        calendar=["2024-12-26","2024-12-27","2024-12-30","2024-12-31","2025-01-02","2025-01-03","2025-01-06"], policy=policy)
    assert quotes["SH600000"].tradestatus == "trading"
    assert result.trades[0]["quantity"] == 900
    assert result.trades[0]["commission"] == "5.00"
    assert result.state.cash >= 0 and close["SH600000"] == 10


def test_reference_audit_distinguishes_virtual_reference_from_actual_cash():
    frame = pd.DataFrame([dict(instrument="SH600000", date="2025-01-03", close="10", preclose="10", source_complete_quote=True),
        dict(instrument="SH600000", date="2025-01-06", close="9.50", preclose="9.51", source_complete_quote=True)])
    event = {"instrument":"SH600000","ex_date":"2025-01-06","cash_per_share":"0.50","bonus_ratio":"0","event_id":"x"}
    calendar=["2025-01-03","2025-01-06"]
    assert a2.audit_ex_references(frame,[event],calendar,start=calendar[0],end=calendar[-1])["status"] == "passed"
    frame.loc[1,"preclose"]="9.55"
    assert a2.audit_ex_references(frame,[event],calendar,start=calendar[0],end=calendar[-1])["unexplained"]
    event["reference_cash_per_share"]="0.45"
    assert not a2.audit_ex_references(frame,[event],calendar,start=calendar[0],end=calendar[-1])["unexplained"]
    assert event["cash_per_share"] == "0.50"


def test_a2_dividend_registration_does_not_double_adjust_carried_marks():
    ex=a2.ex
    calendar=["2025-01-02","2025-01-03","2025-01-06"]
    policy=ex.ExecutionPolicy()
    state=ex.PortfolioState.initial(policy)
    state.positions["SH600000"]=ex.Position(quantity=100,last_mark=Decimal("10"),last_mark_date=calendar[0])
    state.holdings_history[calendar[1]]={"SH600000":100}
    event=dict(instrument="SH600000",event_id="proof",record_date=calendar[1],ex_date=calendar[2],pay_date=calendar[2],listing_date=None,
        cash_per_share="1",bonus_ratio="0",known_at="2025-01-02T00:00:00+08:00",source_response_sha256="a"*64)
    state,ledger=a2._apply_events(state,calendar[2],[event],SimpleNamespace(conversions=lambda day:[]),calendar,{},policy,calendar[0])
    assert state.positions["SH600000"].last_mark == 9
    assert state.cash == Decimal("100080")
    assert len(ledger)==2


def test_training_protocol_rejects_other_dates_or_parameters(tmp_path):
    import json
    from dataclasses import asdict
    keys=("train_start","train_end","valid_start","valid_end","oos_start","oos_end","top_k","rebalance_sessions")
    spec={key:i for i,key in enumerate(keys)}
    payload=dict(config=spec.copy(),model_spec=asdict(a2.FixedQlibLGBSpec()),parameter_search=False,early_stopping=False,
                 label="Ref($open,-6)/Ref($open,-1)-1",feature_schema="qlib.contrib.data.handler.Alpha158 official 158")
    (tmp_path/"protocol.json").write_text(json.dumps(payload));a2.validate_training_protocol(tmp_path,spec)
    payload["model_spec"]["num_leaves"]=15
    (tmp_path/"protocol.json").write_text(json.dumps(payload))
    with pytest.raises(ValueError,match="fixed chronological"):
        a2.validate_training_protocol(tmp_path,spec)


@pytest.mark.parametrize("count",[18,19,280,299,300])
def test_equal_weight_decimal_rounding_cannot_create_leverage(count):
    symbols=[f"SH{600000+i:06d}" for i in range(count)]
    closes=pd.DataFrame({inst:[10.] for inst in symbols},index=["2025-01-03"])
    pool=SimpleNamespace(members_on=lambda day:symbols)
    actions=SimpleNamespace(excluded=lambda *args,**kwargs:False)
    targets,_=a2._targets("equal_weight","2025-01-03",1,closes,pool,None,actions,20)
    assert Decimal(1)-Decimal("1e-15") < sum(targets.values(),Decimal(0)) <= 1
    assert len(set(targets.values()))==1
