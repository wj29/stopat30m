from types import SimpleNamespace

import pytest
from baostock.common.contants import BAOSTOCK_PER_PAGE_COUNT

from stopat30m.data.baostock_paced import PacedBaostockDeadlineClient


def test_row_reads_do_not_sleep_and_lazy_page_requests_share_network_clock(monkeypatch):
    clock = [0.0]
    waits, starts = [], []
    monkeypatch.setattr("stopat30m.data.baostock_paced.time.monotonic", lambda: clock[0])

    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr("stopat30m.data.baostock_paced.time.sleep", sleep)
    result = SimpleNamespace(data=[0] * BAOSTOCK_PER_PAGE_COUNT, cur_row_num=0, fields=["code"], error_code="0")

    def next_row():
        if result.cur_row_num < len(result.data):
            return True
        starts.append(("page", clock[0]))
        result.data = []
        result.cur_row_num = 0
        return False

    def row():
        result.cur_row_num += 1
        return ["sh.600000"]

    result.next, result.get_row_data = next_row, row

    def query():
        starts.append(("query", clock[0]))
        return result

    client = PacedBaostockDeadlineClient(SimpleNamespace(query_history=query))
    observed = client.query_history()
    for _ in range(2):
        assert observed.next()
        assert observed.get_row_data() == ["sh.600000"]
    assert not waits
    result.cur_row_num = BAOSTOCK_PER_PAGE_COUNT
    assert observed.next() is False
    assert waits == [0.6]
    client.query_history()
    assert starts == [("query", 0.0), ("page", 0.6), ("query", 1.2)]
    assert waits == [0.6, 0.6]


def test_short_or_oversized_terminal_page_does_not_wait(monkeypatch):
    for count in [0, 7, BAOSTOCK_PER_PAGE_COUNT + 1]:
        result = SimpleNamespace(data=[0] * count, cur_row_num=count,
                                 next=lambda: False, get_row_data=lambda: [])
        client = PacedBaostockDeadlineClient(result)
        client._pacing["last_start"] = 1e30
        monkeypatch.setattr("stopat30m.data.baostock_paced.time.sleep",
                            lambda _: pytest.fail("No network request on a terminal SDK page"))
        assert client.next() is False


def test_bulk_get_data_cannot_hide_unpaced_internal_requests():
    client = PacedBaostockDeadlineClient(SimpleNamespace(get_data=lambda: "would request pages"))
    with pytest.raises(ValueError, match="every network page is paced"):
        client.get_data()


@pytest.mark.parametrize("interval", [0, 0.49, -1, 61, float("nan"), float("inf"), True, "0.6"])
def test_invalid_network_interval(interval):
    with pytest.raises(ValueError):
        PacedBaostockDeadlineClient(SimpleNamespace(), min_interval=interval)
