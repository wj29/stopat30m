from copy import deepcopy
from types import SimpleNamespace

import pytest

from stopat30m.data.ashare_daily import _rows
from stopat30m.data.baostock_validated import ValidatedBaostockDividendClient


class Result:
    fields = ["code", "dividOperateDate"]
    error_code = "0"
    code, year, yearType = "sz.000756", "2024", "operate"

    def __init__(self, rows):
        self.data, self.cur_row_num = rows, 0

    def next(self):
        return self.cur_row_num < len(self.data)

    def get_row_data(self):
        row = self.data[self.cur_row_num]
        self.cur_row_num += 1
        return row


def client_for(result, monkeypatch):
    discarded, guards = [], []
    monkeypatch.setattr("stopat30m.data.baostock_validated._discard_connection",
                        lambda: discarded.append(True))
    source = SimpleNamespace(query_dividend_data=lambda **kwargs: result)
    client = ValidatedBaostockDividendClient(source, on_guard=lambda x: guards.append(deepcopy(x)))
    return client, discarded, guards


@pytest.mark.parametrize("rows", [[], [["sz.000756", "2024-07-26"]], [["sz.000756", ""]]])
def test_correct_server_echo_and_rows_are_preserved(rows, monkeypatch):
    result = Result(deepcopy(rows))
    client, discarded, guards = client_for(result, monkeypatch)
    observed = _rows(client.query_dividend_data(code="sz.000756", year="2024", yearType="operate"))
    assert observed == rows
    assert not discarded
    assert guards[-1]["status"] == "verified"
    assert guards[-1]["rows_verified"] == len(rows)
    assert guards[-1]["server_echo"] == {"code": "sz.000756", "year": "2024", "yearType": "operate"}


@pytest.mark.parametrize("attribute,value", [("code", "sz.000758"), ("year", "2023"), ("yearType", "report")])
def test_wrong_server_echo_including_empty_stale_response_is_rejected(attribute, value, monkeypatch):
    result = Result([])
    setattr(result, attribute, value)
    client, discarded, guards = client_for(result, monkeypatch)
    with pytest.raises(ValueError, match="echo differs"):
        client.query_dividend_data(code="sz.000756", year="2024", yearType="operate")
    assert discarded == [True]
    assert guards[-1]["status"] == "invalid"


@pytest.mark.parametrize("row", [["sz.000756", "2023-07-26"], ["sz.000758", "2024-07-26"],
                                  ["sz.000756", "2024-99-99"]])
def test_wrong_row_year_security_or_date_cannot_be_captured(row, monkeypatch):
    client, discarded, guards = client_for(Result([row]), monkeypatch)
    with pytest.raises(ValueError):
        _rows(client.query_dividend_data(code="sz.000756", year="2024", yearType="operate"))
    assert discarded == [True]
    assert guards[-1]["status"] == "invalid"
    assert guards[-1]["rows_verified"] == 0


def test_unsuccessful_server_response_discards_socket(monkeypatch):
    result = Result([])
    result.error_code = "10002007"
    client, discarded, guards = client_for(result, monkeypatch)
    with pytest.raises(ValueError, match="unsuccessful"):
        client.query_dividend_data(code="sz.000756", year="2024", yearType="operate")
    assert discarded == [True]
    assert guards[-1]["server_error_code"] == "10002007"


def test_invalid_result_marks_shared_client_dirty_and_never_reuses_sdk(monkeypatch):
    discarded, calls = [], []
    monkeypatch.setattr("stopat30m.data.baostock_validated._discard_connection",
                        lambda: discarded.append(True))
    result = Result([["sz.000756", "2023-07-26"]])

    def query(**kwargs):
        calls.append(kwargs)
        return result

    client = ValidatedBaostockDividendClient(SimpleNamespace(query_dividend_data=query))
    with pytest.raises(ValueError, match="ex-date year"):
        _rows(client.query_dividend_data(code="sz.000756", year="2024"))
    with pytest.raises(ValueError, match="dirty"):
        client.query_dividend_data(code="sz.000756", year="2024")
    assert len(calls) == 1
