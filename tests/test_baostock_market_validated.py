from copy import deepcopy
from types import SimpleNamespace

import pytest

from stopat30m.data.ashare_daily import _rows
from stopat30m.data.baostock_market_validated import ValidatedBaostockMarketClient


class Result:
    fields = ["date", "code", "close", "adjustflag"]
    code, start_date, end_date, frequency, adjustflag = "sh.688169", "2019-07-22", "2026-10-19", "d", "1"
    error_code = "0"

    def __init__(self, rows):
        self.data, self.cur_row_num = rows, 0

    def next(self):
        return self.cur_row_num < len(self.data)

    def get_row_data(self):
        row = self.data[self.cur_row_num]
        self.cur_row_num += 1
        return row


REQUEST = dict(code="sh.688169", fields=",".join(Result.fields), start_date="2019-07-22",
               end_date="2026-10-19", frequency="d", adjustflag="1")


def client_for(result, monkeypatch):
    guards, discarded = [], []
    monkeypatch.setattr("stopat30m.data.baostock_validated._discard_connection",
                        lambda: discarded.append(True))
    client = ValidatedBaostockMarketClient(SimpleNamespace(query_history_k_data_plus=lambda **kw: result),
        on_guard=lambda x: guards.append(deepcopy(x)))
    return client, guards, discarded


def test_original_ipo_row_mode_anomaly_is_observed_without_relabelling(monkeypatch):
    rows = [["2019-07-22", "sh.688169", "47.55", "3"], ["2019-07-23", "sh.688169", "47.71", "1"]]
    client, guards, discarded = client_for(Result(deepcopy(rows)), monkeypatch)
    assert _rows(client.query_history_k_data_plus(**REQUEST)) == rows
    assert not discarded
    assert guards[-1]["status"] == "verified"
    assert guards[-1]["row_mode_flags_repaired"] is False
    assert guards[-1]["row_mode_anomalies_observed"] == [
        {"date": "2019-07-22", "requested_mode": "1", "returned_row_mode": "3"}]


@pytest.mark.parametrize("attribute,value", [("code", "sh.688168"), ("adjustflag", "3"),
    ("start_date", "2020-01-01"), ("end_date", "2025-12-31"), ("frequency", "w")])
def test_late_response_with_another_request_echo_is_rejected(attribute, value, monkeypatch):
    result = Result([])
    setattr(result, attribute, value)
    client, guards, discarded = client_for(result, monkeypatch)
    with pytest.raises(ValueError, match="echo differs"):
        client.query_history_k_data_plus(**REQUEST)
    assert discarded == [True]
    assert guards[-1]["status"] == "invalid"


def test_wrong_row_date_discards_connection_before_any_capture(monkeypatch):
    client, guards, discarded = client_for(Result([["2018-07-22", "sh.688169", "47.55", "1"]]), monkeypatch)
    with pytest.raises(ValueError, match="outside"):
        _rows(client.query_history_k_data_plus(**REQUEST))
    with pytest.raises(ValueError, match="dirty"):
        client.query_history_k_data_plus(**REQUEST)
    assert discarded


def test_live_protocol_empty_mode_header_is_preserved_and_disclosed(monkeypatch):
    result = Result([["2019-07-22", "sh.688169", "47.55", "3"],
                     ["2019-07-23", "sh.688169", "47.71", "1"]])
    result.adjustflag = ""
    client, guards, discarded = client_for(result, monkeypatch)
    assert _rows(client.query_history_k_data_plus(**REQUEST)) == result.data
    assert not discarded
    assert guards[-1]["server_echo"]["adjustflag"] == ""
    assert guards[-1]["mode_server_echo_present"] is False
    assert "server_mode_echo_absent" in guards[-1]["mode_validation_basis"]
    assert guards[-1]["row_mode_anomalies_observed"][0]["returned_row_mode"] == "3"


def metadata_result(fields, request, rows):
    result = Result(rows)
    result.fields = fields
    for key, value in request.items():
        setattr(result, key, value)
    return result


def test_factor_echo_rows_and_terminal_iteration_are_bound(monkeypatch):
    request = {"code": "sh.688169", "start_date": "2000-01-04", "end_date": "2026-10-19"}
    fields = ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"]
    rows = [["sh.688169", "2019-07-23", "0.5", "1", "1"]]
    result = metadata_result(fields, request, rows)
    client, guards, discarded = client_for(result, monkeypatch)
    client._source.query_adjust_factor = lambda **kw: result
    assert _rows(client.query_adjust_factor(**request)) == rows
    assert not discarded
    assert guards[-1]["server_echo"] == request
    assert guards[-1]["rows_verified"] == 1
    assert guards[-1]["status"] == "verified"


def test_metadata_transport_failure_prevents_later_daily_request(monkeypatch):
    client, guards, discarded = client_for(Result([]), monkeypatch)
    def fail(**kwargs):
        raise TimeoutError("delayed response may remain")
    client._source.query_stock_basic = fail
    with pytest.raises(ValueError, match="Metadata SDK request failed"):
        client.query_stock_basic()
    with pytest.raises(ValueError, match="dirty"):
        client.query_history_k_data_plus(**REQUEST)
    assert discarded


def test_empty_stale_factor_echo_is_rejected(monkeypatch):
    request = {"code": "sh.688169", "start_date": "2000-01-04", "end_date": "2026-10-19"}
    result = metadata_result(["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
        {**request, "code": "sh.688168"}, [])
    client, guards, discarded = client_for(result, monkeypatch)
    client._source.query_adjust_factor = lambda **kw: result
    with pytest.raises(ValueError, match="echo/schema"):
        client.query_adjust_factor(**request)
    assert discarded


def test_basic_query_can_observe_multiple_codes_without_claiming_one_security(monkeypatch):
    fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
    rows = [["sh.688169", "A", "2019-07-22", "", "1", "1"],
            ["sz.000001", "B", "1991-04-03", "", "1", "1"]]
    result = metadata_result(fields, {"code": "", "code_name": ""}, rows)
    client, guards, discarded = client_for(result, monkeypatch)
    client._source.query_stock_basic = lambda **kw: result
    assert _rows(client.query_stock_basic()) == rows
    assert guards[-1]["rows_verified"] == 2
    assert not discarded
