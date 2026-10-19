"""Original collection identity/resume checks; synthetic values are not research results."""
import json
from types import SimpleNamespace

import pytest

from stopat30m.data import ashare_daily as daily


class Response:
    error_code = "0"
    def __init__(self, fields, rows):
        self.fields, self.rows, self.position = fields, rows, -1
    def next(self):
        self.position += 1
        return self.position < len(self.rows)
    def get_row_data(self):
        return self.rows[self.position]


class Client:
    def __init__(self, fail=False):
        self.requests, self.login_calls, self.fail = [], 0, fail
    def login(self):
        self.login_calls += 1
        return SimpleNamespace(error_code="0")
    def logout(self):
        pass
    def query_hs300_stocks(self, date):
        return Response(["updateDate", "code", "code_name"], [[date, "sh.600000", "fixture"]])
    def query_history_k_data_plus(self, **request):
        self.requests.append(request)
        if self.fail:
            raise TimeoutError("Synthetic source unavailable")
        p = "10" if request["adjustflag"] == "3" else "20"
        row = ["2026-10-16", request["code"], p, p, p, p, "100", "1000",
               request["adjustflag"], "1", "0"]
        return Response(daily.FIELDS.split(","), [row])


@pytest.fixture(autouse=True)
def no_delay(monkeypatch):
    monkeypatch.setattr(daily.time, "sleep", lambda _: None)


def test_original_modes_are_distinct_and_repeated_complete_generation_is_offline(tmp_path):
    client = Client()
    root = daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    raw, hfq, manifest = daily.load_collection_frames(root)
    assert raw.close.tolist() == ["10"] and hfq.close.tolist() == ["20"]
    assert manifest["target_selection"]["historical_membership_proven"] is False
    assert len(client.requests) == 2
    before = (root / "collection.json").read_bytes()
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    assert client.login_calls == 1 and len(client.requests) == 2
    assert (root / "collection.json").read_bytes() == before


def test_changed_collection_request_cannot_reuse_original_responses(tmp_path):
    client = Client()
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    with pytest.raises(ValueError, match="Resume request"):
        daily.collect_baostock_increment(tmp_path, "2026-09-01", "2026-10-19", client=client)
    assert client.login_calls == 1


def test_tampered_original_bytes_are_rejected_before_offline_reuse(tmp_path):
    client = Client()
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    file = tmp_path / "SH600000_raw.json"
    value = json.loads(file.read_text()); value["rows"][0][5] = "999"
    file.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="identity/checksum"):
        daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    assert client.login_calls == 1


def test_three_consecutive_failures_stop_the_pool_and_cannot_publish(tmp_path):
    client = Client(fail=True)
    symbols = [f"SH60000{i}" for i in range(5)]
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client, instruments=symbols)
    assert len(client.requests) == 3
    manifest = json.loads((tmp_path / "collection.json").read_text())
    assert manifest["status"] == "partial_not_publishable"
    with pytest.raises(ValueError, match="incomplete"):
        daily.load_collection_frames(tmp_path)


def test_collection_cannot_be_relabelled_as_another_source(tmp_path):
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=Client())
    path = tmp_path / "collection.json"
    manifest = json.loads(path.read_text()); manifest["source"] = "different"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="source"):
        daily.load_collection_frames(tmp_path)


def test_real_sdk_silent_full_page_socket_failure_cannot_be_cached_as_complete(tmp_path, monkeypatch):
    from baostock.common import contants as cons
    from baostock.data import resultset

    class PagedClient(Client):
        def query_history_k_data_plus(self, **request):
            response = resultset.ResultData()
            response.error_code = "0"
            response.msg_type = "1000"
            response.cur_page_num = "1"
            response.msg_body = cons.MESSAGE_SPLIT.join(["query", "user", "1"])
            response.fields = daily.FIELDS.split(",")
            row = ["2026-10-16", request["code"], "10", "10", "10", "10", "100", "1000",
                   request["adjustflag"], "1", "0"]
            response.data = [row] * cons.BAOSTOCK_PER_PAGE_COUNT
            return response

    monkeypatch.setattr(resultset.sock, "send_msg", lambda *_: None)
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=PagedClient())
    manifest = json.loads((tmp_path / "collection.json").read_text())
    assert manifest["status"] == "partial_not_publishable"
    assert manifest["responses"] == {}
    assert "terminal page" in manifest["failures"]["SH600000"]["error"]
    assert not (tmp_path / "SH600000_raw.json").exists()


def test_real_sdk_short_terminal_page_does_not_require_another_request(monkeypatch):
    from baostock.data import resultset

    response = resultset.ResultData()
    response.error_code = "0"
    response.fields = ["date"]
    response.data = [["2026-10-16"]]
    monkeypatch.setattr(resultset.sock, "send_msg", lambda *_: pytest.fail("short page is terminal"))
    assert daily._rows(response) == [["2026-10-16"]]


def test_successful_response_pair_with_missing_page_dates_cannot_be_published(tmp_path):
    class MismatchedClient(Client):
        def query_history_k_data_plus(self, **request):
            response = super().query_history_k_data_plus(**request)
            if request["adjustflag"] == "1":
                extra = response.rows[0].copy()
                extra[0] = "2026-10-19"
                response.rows.append(extra)
            return response

    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=MismatchedClient())
    with pytest.raises(ValueError, match="raw/HFQ dates differ"):
        daily.load_collection_frames(tmp_path)


def test_collection_cannot_pollute_a_frozen_snapshot_even_with_a_new_subdirectory(tmp_path):
    frozen = tmp_path / "snapshot"
    frozen.mkdir()
    manifest = frozen / "manifest.json"
    manifest.write_text("{}")
    client = Client()
    with pytest.raises(ValueError, match="immutable"):
        daily.collect_baostock_increment(frozen / "new_collection", "2026-08-01", "2026-10-19", client=client)
    assert list(frozen.iterdir()) == [manifest]
    assert client.login_calls == 0


def test_changed_index_date_cannot_silently_reuse_an_old_target_pool(tmp_path):
    client = Client()
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", index_date="2026-10-16", client=client)
    original = (tmp_path / "collection.json").read_bytes()
    with pytest.raises(ValueError, match="selection/date differs"):
        daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", index_date="2026-10-19", client=client)
    assert client.login_calls == 1
    assert (tmp_path / "collection.json").read_bytes() == original


def test_original_download_target_evidence_is_verified_on_offline_reuse(tmp_path):
    client = Client()
    daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    evidence = tmp_path / "download_targets.json"
    metadata = json.loads(evidence.read_text())
    metadata["requested_date"] = "2020-01-01"
    evidence.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="evidence checksum"):
        daily.collect_baostock_increment(tmp_path, "2026-08-01", "2026-10-19", client=client)
    assert client.login_calls == 1
