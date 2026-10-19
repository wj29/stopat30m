"""Cash/share event dates and original dividend responses remain auditable."""
import json
from types import SimpleNamespace

import pytest

from stopat30m.data import ashare_actions as actions


class Response:
    error_code = "0"
    code = "sh.600000"
    def __init__(self, row):
        self.fields = sorted(actions.REQUIRED | {"dividCashPsAfterTax"})
        self.rows = [[row.get(key, "") for key in self.fields]] if row else []
        self.n = 0
    def next(self):
        return self.n < len(self.rows)
    def get_row_data(self):
        row = self.rows[self.n]; self.n += 1; return row


class Source:
    def __init__(self, row):
        self.row, self.requests, self.logins = row, [], 0
    def login(self):
        self.logins += 1; return SimpleNamespace(error_code="0")
    def logout(self):
        pass
    def query_dividend_data(self, **request):
        self.requests.append(request)
        return Response(self.row)


@pytest.fixture
def row():
    return {"code": "sh.600000", "dividPlanDate": "2025-06-01", "dividRegistDate": "2025-06-06",
        "dividOperateDate": "2025-06-09", "dividPayDate": "2025-06-10", "dividStockMarketDate": "2025-06-11",
        "dividCashPsBeforeTax": "0.50", "dividStocksPs": "0", "dividReserveToStockPs": "0.1"}


def test_dividends_query_ex_date_year_and_preserve_pay_and_bonus_availability(tmp_path, row):
    source = Source(row)
    root = actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=source)
    assert source.requests == [{"code": "sh.600000", "year": "2025", "yearType": "operate"}]
    events = actions.dividend_events(root, start="2025-01-01", end="2025-12-31")
    assert events[0]["cash_per_share"] == "0.50" and events[0]["bonus_ratio"] == "0.1"
    assert events[0]["record_date"] == "2025-06-06"
    assert events[0]["ex_date"] == "2025-06-09"
    assert events[0]["pay_date"] == "2025-06-10" and events[0]["listing_date"] == "2025-06-11"
    before = (root / "collection.json").read_bytes()
    actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=source)
    assert source.logins == 1 and (root / "collection.json").read_bytes() == before


def test_missing_required_pay_date_cannot_be_guessed_from_ex_date(tmp_path, row):
    row["dividPayDate"] = ""
    actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=Source(row))
    with pytest.raises(ValueError, match="date"):
        actions.dividend_events(tmp_path, start="2025-01-01", end="2025-12-31")


def test_no_reported_events_is_a_verified_empty_response_not_a_failed_query(tmp_path):
    actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=Source({}))
    assert actions.dividend_events(tmp_path, start="2025-01-01", end="2025-12-31") == []


def test_draft_or_outside_period_actions_do_not_create_retrospective_cash(tmp_path, row):
    row["dividOperateDate"] = ""
    actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=Source(row))
    assert actions.dividend_events(tmp_path, start="2025-01-01", end="2025-12-31") == []


def test_tampered_original_dividend_response_is_not_reused(tmp_path, row):
    source = Source(row)
    actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=source)
    file = tmp_path / "SH600000_2025.json"
    value = json.loads(file.read_text()); value["rows"][0][0] = "changed"
    file.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="identity"):
        actions.collect_dividend_evidence(tmp_path, ["SH600000"], [2025], client=source)
    assert source.logins == 1


def test_identical_implemented_duplicate_uses_complete_disclosure_without_guessing(monkeypatch, row):
    import pandas as pd
    complete={**row,"instrument":"SH600000","source_response_sha256":"a"*64}
    incomplete={**complete,"dividPlanDate":"","dividCashPsAfterTax":""}
    monkeypatch.setattr(actions,"load_dividend_evidence",lambda path:(pd.DataFrame([complete,incomplete]).fillna(""),{"generation":"g"}))
    events=actions.dividend_events("unused",start="2025-01-01",end="2025-12-31")
    assert len(events)==1 and events[0]["known_at"]=="2025-06-01T23:59:59+08:00"
    incomplete["dividCashPsBeforeTax"]="0.60"
    with pytest.raises(ValueError,match="date"):
        actions.dividend_events("unused",start="2025-01-01",end="2025-12-31")


def test_issuer_reference_annotation_cannot_change_cash_and_binds_proof_bytes(tmp_path):
    import hashlib
    proof=tmp_path/"issuer.txt";proof.write_text("Fixture issuer cash .50; virtual reference .45")
    original=dict(event_id="supplier",instrument="SH600000",record_date="2025-06-06",ex_date="2025-06-09",pay_date="2025-06-09",
                  listing_date=None,cash_per_share="0.50",bonus_ratio="0",known_at="2025-06-01T23:59:59+08:00",source_response_sha256="a"*64)
    patch=dict(instrument="SH600000",ex_date="2025-06-09",operation="annotate",values={"reference_cash_per_share":"0.45"},
               evidence={"path":str(proof),"sha256":hashlib.sha256(proof.read_bytes()).hexdigest(),"url":"https://example.test/issuer"})
    target=tmp_path/"overrides.json";target.write_text(json.dumps({"patches":[patch]}))
    event=actions.apply_action_overrides([original],target)[0]
    assert event["cash_per_share"]=="0.50" and event["reference_cash_per_share"]=="0.45"
    patch["values"]["cash_per_share"]="0.99";target.write_text(json.dumps({"patches":[patch]}))
    with pytest.raises(ValueError,match="cannot change"):
        actions.apply_action_overrides([original],target)
    patch["values"].pop("cash_per_share");target.write_text(json.dumps({"patches":[patch]}));proof.write_text("tampered")
    with pytest.raises(ValueError,match="bytes"):
        actions.apply_action_overrides([original],target)
