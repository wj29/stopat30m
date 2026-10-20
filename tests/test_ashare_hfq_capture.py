import json
from types import SimpleNamespace

import pytest

from stopat30m.data import ashare_hfq_capture as capture
from stopat30m.data.ashare_execution_data import EXECUTION_FIELDS


class Source:
    def __init__(self, *, flag="1", failure=False):
        self.flag, self.failure, self.calls = flag, failure, []
        self.logged_out = False

    def login(self):
        return SimpleNamespace(error_code="0")

    def logout(self):
        self.logged_out = True

    def query_history_k_data_plus(self, **request):
        self.calls.append(request)
        if self.failure:
            raise TimeoutError("bounded source failure")
        row = [request["end_date"], request["code"], "10", "11", "9", "10", "10",
               "100", "1000", self.flag, "1", "0"]
        return Result(row)


class Result:
    fields, error_code = EXECUTION_FIELDS.split(","), "0"

    def __init__(self, row):
        self.row, self.used = row, False

    def next(self):
        return not self.used

    def get_row_data(self):
        self.used = True
        return self.row


def setup(tmp_path, monkeypatch):
    selection = tmp_path / "selection.json"
    selection.write_text('{"source_only":true}')
    monkeypatch.setattr(capture.time, "sleep", lambda _: None)
    return tmp_path / "capture", selection


def test_only_hfq_queries_preserve_individual_windows_and_complete_resume(tmp_path, monkeypatch):
    root, selection = setup(tmp_path, monkeypatch)
    windows = {"SH600000": {"start_date": "2026-08-14", "end_date": "2026-10-19"},
               "SZ300773": {"start_date": "2026-08-05", "end_date": "2026-10-19"}}
    source = Source()
    capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=source)
    frame, receipt = capture.load_hfq_evidence(root)
    assert len(frame) == 2 and source.logged_out
    assert [r["start_date"] for r in source.calls] == ["2026-08-14", "2026-08-05"]
    assert all(r["adjustflag"] == "1" and r["fields"] == EXECUTION_FIELDS for r in source.calls)
    assert receipt["raw_prices_downloaded"] is False and receipt["qlib_seam_verified"] is False
    source2 = Source(failure=True)
    capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=source2)
    assert source2.calls == []
    payload = root / "SH600000_hfq.json"
    payload.write_bytes(payload.read_bytes() + b" ")
    with pytest.raises(ValueError, match="bytes/request"):
        capture.load_hfq_evidence(root)


def test_wrong_original_mode_is_retained_and_cannot_be_overwritten_on_resume(tmp_path, monkeypatch):
    root, selection = setup(tmp_path, monkeypatch)
    windows = {"SH600000": {"start_date": "2026-08-14", "end_date": "2026-10-19"}}
    capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=Source(flag="3"))
    before = (root / "SH600000_hfq.json").read_bytes()
    receipt = json.loads((root / "collection.json").read_text())
    assert receipt["status"] == "partial_not_publishable" and "SH600000" in receipt["failures"]
    assert json.loads(before)["rows"][0][9] == "3"
    source = Source()
    capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=source)
    assert source.calls == [] and (root / "SH600000_hfq.json").read_bytes() == before
    with pytest.raises(ValueError, match="Incomplete"):
        capture.load_hfq_evidence(root)


def test_three_consecutive_failures_stop_without_attempting_later_targets(tmp_path, monkeypatch):
    root, selection = setup(tmp_path, monkeypatch)
    windows = {"SH60000" + str(i): {"start_date": "2026-08-14", "end_date": "2026-10-19"}
               for i in range(5)}
    source = Source(failure=True)
    capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=source)
    receipt = json.loads((root / "collection.json").read_text())
    assert len(source.calls) == 3 and source.logged_out
    assert receipt["responses"] == {} and receipt["stopped_after_consecutive_failures"] == 3
    assert receipt["status"] == "partial_not_publishable"


def test_resume_rejects_mutated_selection_before_any_network(tmp_path, monkeypatch):
    root, selection = setup(tmp_path, monkeypatch)
    windows = {"SH600000": {"start_date": "2026-08-14", "end_date": "2026-10-19"}}
    capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=Source())
    selection.write_text('{"different":true}')
    source = Source()
    with pytest.raises(ValueError, match="selection differs"):
        capture.collect_hfq_evidence(root, windows, selection_evidence_path=selection, client=source)
    assert source.calls == []
