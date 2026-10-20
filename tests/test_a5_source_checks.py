"""Indexed orchestration must preserve original source-check outcomes exactly."""
import numpy as np
import pandas as pd
import pytest

from stopat30m.data import a5_source_checks as indexed, ashare_history_audit as original


@pytest.fixture
def source():
    days = ["2022-12-30", "2023-01-03", "2023-01-04", "2023-01-05"]
    names = ["SH600000", "SZ000001"]
    raw = pd.DataFrame([{"instrument": name, "date": day, "open": "10", "high": "10", "low": "10",
        "close": "10", "preclose": "10", "volume": "100", "amount": "1000", "tradestatus": "1", "isST": "0",
        "source_complete_quote": True, "source_observed_market_quote": True, "source_response_sha256": "fixture"}
        for name in names for day in days])
    basic = pd.DataFrame([{"code": "sh.600000", "ipoDate": "1999-11-10", "outDate": ""},
                          {"code": "sz.000001", "ipoDate": "1991-04-03", "outDate": ""}])
    return raw, basic, days, names


@pytest.mark.parametrize("case", ["clean", "missing", "halt", "unknown_st", "out", "duplicate"])
def test_same_daily_status_identity_and_failure_order(source, case):
    raw, basic, days, names = source
    if case == "missing": raw = raw.drop(2)
    if case == "halt": raw.loc[2, ["tradestatus", "source_complete_quote", "source_observed_market_quote"]] = ["0", False, True]
    if case == "unknown_st": raw.loc[4, "isST"] = "unknown"
    if case == "out": basic.loc[0, "outDate"] = days[-1]
    if case == "duplicate": raw = pd.concat([raw, raw.iloc[:1]])
    kwargs = {"start": "2023-01-01", "end": "2023-12-31"}
    if case == "duplicate":
        for check in (indexed.audit_daily_coverage, original.audit_daily_coverage):
            with pytest.raises(ValueError, match="Duplicate"): check(raw,basic,days,names,**kwargs)
    else:
        assert indexed.audit_daily_coverage(raw,basic,days,names,**kwargs) == original.audit_daily_coverage(raw,basic,days,names,**kwargs)


def test_same_model_availability_with_real_gap_and_halt(source, monkeypatch):
    raw, _, days, names = source
    raw.loc[2, ["tradestatus", "source_complete_quote", "source_observed_market_quote"]] = ["0", False, False]
    def prices(*args):
        frame = pd.DataFrame({field:10. for field in ("open","high","low","close")},index=days)
        frame.loc[days[2]] = np.nan
        frame.loc[days[3], "open"] = np.nan
        return frame
    monkeypatch.setattr(original,"read_qlib_security",prices)
    args = ("fixture",raw,names)
    kwargs = {"start":days[0],"end":days[-1]}
    assert indexed.audit_qlib_availability(*args,**kwargs) == original.audit_qlib_availability(*args,**kwargs)


def test_reference_crosses_halt_and_checks_multiple_same_day_entitlements(source):
    raw, _, days, names = source
    raw.loc[1, ["source_observed_market_quote", "source_complete_quote", "tradestatus"]] = [False,False,"0"]
    raw.loc[2, ["preclose","close"]] = ["8","8"]
    raw.loc[3, ["preclose","close"]] = ["8","8"]
    events=[{"event_id":str(n),"instrument":names[0],"ex_date":days[1],"cash_per_share":"1","bonus_ratio":"0"} for n in range(2)]
    kwargs={"start":"2023-01-01","end":"2023-12-31"}
    calendar=["2017-01-03",*days,"2024-01-02"]
    expected=original.audit_reference_prices(raw,events,calendar,**kwargs)
    assert indexed.audit_reference_prices(raw,events,calendar,**kwargs)==expected
    assert expected["status"]=="passed"


def test_unused_or_duplicate_rights_still_fail(source):
    raw, _, days, names = source
    kwargs={"start":"2023-01-01","end":"2023-12-31"}
    right={"instrument":"SH999999","ex_date":days[1]}
    for check in (indexed.audit_reference_prices,original.audit_reference_prices):
        with pytest.raises(ValueError,match="Every rights"): check(raw,[],days,rights=[right],**kwargs)


def test_independent_rights_on_two_stocks_keep_same_reference_validation(source):
    raw, _, days, names = source
    rights=[{"instrument":name,"ex_date":days[1],"event_id":"rights:"+name,
        "actual_issued_ratio":".1","subscription_price":"10","independent_reference_verified":True,
        "reference_preclose":"10"} for name in names]
    kwargs={"start":"2023-01-01","end":"2023-12-31"}
    expected=original.audit_reference_prices(raw,[],days,rights=rights,**kwargs)
    assert expected["status"]=="passed"
    assert indexed.audit_reference_prices(raw,[],days,rights=rights,**kwargs)==expected
    for check in (indexed.audit_reference_prices,original.audit_reference_prices):
        with pytest.raises(ValueError,match="Duplicate rights"): check(raw,[],days,rights=rights+rights[:1],**kwargs)
