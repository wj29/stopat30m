"""Sina price crosschecks preserve uncertainty instead of inventing status."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from stopat30m.data.sina_execution import (
    collect_sina_crosscheck, decode_sina_response, load_sina_crosscheck, normalize_sina_rows,
)


def rows():
    return [
        {"date":"2024-06-18T00:00:00Z", "open":15, "high":16, "low":15, "close":16,
         "volume":1000, "amount":15500},
        {"date":"2024-06-19T00:00:00Z", "open":15, "high":16, "low":14, "close":15,
         "volume":1000, "amount":15000, "prevclose":15, "postVol":10, "postAmt":150},
        {"date":"2024-06-20T00:00:00Z", "open":15, "high":16, "low":14, "close":15.1,
         "volume":1000, "amount":15000},
    ]


def normalize(value=None, **kwargs):
    return normalize_sina_rows(value or rows(), "SH600519", start_date="2024-06-18", end_date="2024-06-20", **kwargs)


def test_exceptional_reference_is_preserved_and_prior_raw_close_never_substitutes():
    first,ex,ordinary = normalize()
    assert ex["explicit_prevclose"] == "15"
    assert ex["previous_observed_close"] == "16"
    assert ex["reference_price_candidate"] == "15"
    assert ex["reference_verified"] is True
    assert ordinary["explicit_prevclose"] is None
    assert ordinary["reference_price_candidate"] == "15"
    assert ordinary["reference_verified"] is False
    assert first["reference_price_candidate"] is None
    for observation in (first, ex, ordinary):
        assert observation["tradestatus"] is None and observation["isST"] is None
        assert observation["execution_evidence_complete"] is False


def test_cropping_keeps_the_actual_preceding_observation_without_filling_missing_dates():
    data=rows();data.pop(1)
    result=normalize_sina_rows(data,"SH600519",start_date="2024-06-19",end_date="2024-06-21")
    assert len(result)==1 and result[0]["date"]=="2024-06-20"
    assert result[0]["previous_observed_session"]=="2024-06-18"
    assert result[0]["reference_verified"] is False


def test_zero_volume_is_not_a_verified_halt_or_a_complete_quote():
    data=rows();data[0].update(volume=0,amount=0)
    result=normalize(data)
    assert result[0]["source_complete_quote"] is False
    assert result[0]["tradestatus"] is None


@pytest.mark.parametrize("field,value",[("close",0),("volume",1.5),("amount",float("inf")),("prevclose",0),("volume",True)])
def test_invalid_original_units_and_reference_prices_are_rejected(field,value):
    data=rows();data[1][field]=value
    with pytest.raises(ValueError): normalize(data)


def test_share_amount_bounds_are_checked():
    data=rows();data[1]["amount"]=1600000
    with pytest.raises(ValueError,match="does not match"):normalize(data)


def test_duplicate_or_reversed_dates_and_wrong_security_are_rejected():
    data=rows();data.append(deepcopy(data[0]))
    with pytest.raises(ValueError,match="increasing"):normalize(data)
    with pytest.raises(ValueError,match="SH/SZ"):
        normalize_sina_rows(rows(),"600519",start_date="2024-06-18",end_date="2024-06-20")


@pytest.mark.parametrize("raw",[
    b'var KLC_K2_sh600520="aaaa";',
    b'var KLC_K2_sh600519="aaaa"; steal_credentials();',
    b'<html>Forbidden</html>',
])
def test_response_identity_and_executable_content_are_rejected_before_decoder_import(raw):
    with pytest.raises(ValueError,match="Wrong security"):
        decode_sina_response(raw,"SH600519")


class Session:
    def __init__(self,status=200):self.status,self.calls=status,0
    def get(self,url,**kwargs):
        self.calls+=1
        assert kwargs["allow_redirects"] is False and kwargs["timeout"] <=60
        return SimpleNamespace(status_code=self.status,content=b'var KLC_K2_sh600519="fixture";')


def collect(path,session=None):
    return collect_sina_crosscheck("SH600519",path,start_date="2024-06-18",end_date="2024-06-20",
        session=session or Session(),decoder=lambda raw,instrument: rows(),
        now=datetime(2026,10,20,tzinfo=timezone.utc))


def test_original_bytes_decoded_rows_and_unknown_status_survive_reload(tmp_path):
    receipt=collect(tmp_path/"sina")
    normalized,reloaded=load_sina_crosscheck(tmp_path/"sina")
    assert receipt==reloaded and len(normalized)==3
    assert (tmp_path/"sina/response.js").read_bytes()==b'var KLC_K2_sh600519="fixture";'
    assert normalized[1]["explicit_prevclose"]=="15"
    with pytest.raises(ValueError,match="new empty"): collect(tmp_path/"sina")


def test_rate_limit_records_original_failure_without_retry(tmp_path):
    session=Session(429)
    with pytest.raises(ValueError,match="429"):
        collect(tmp_path/"sina",session)
    assert session.calls==1 and (tmp_path/"sina/failure.json").is_file()
    assert not (tmp_path/"sina/receipt.json").exists()


def test_saved_evidence_or_claim_tampering_fails_closed(tmp_path):
    collect(tmp_path/"sina")
    receipt=tmp_path/"sina/receipt.json"
    content=json.loads(receipt.read_text());content["historical_ST_available"]=True
    receipt.write_text(json.dumps(content))
    with pytest.raises(ValueError,match="unsupported execution claims"):load_sina_crosscheck(tmp_path/"sina")
    content["historical_ST_available"]=False;receipt.write_text(json.dumps(content))
    (tmp_path/"sina/decoded.json").write_text("[]")
    with pytest.raises(ValueError,match="checksum"):load_sina_crosscheck(tmp_path/"sina")
