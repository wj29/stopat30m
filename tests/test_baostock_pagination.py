from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m.data.fetcher.baostock import BaoStockFetcher, _result_frame


class Response:
    def __init__(self, frame, fail_after=None):
        self.fields = list(frame.columns)
        self.rows = list(frame.astype(str).itertuples(index=False, name=None))
        self.index = -1
        self.error_code = "0"
        self.error_msg = "success"
        self.fail_after = fail_after

    def next(self):
        self.index += 1
        if self.fail_after is not None and self.index >= self.fail_after:
            self.error_code, self.error_msg = "network-error", "page interrupted"
            return False
        return self.index < len(self.rows)

    def get_row_data(self):
        return self.rows[self.index]

    def get_data(self):
        pytest.fail("The removed pandas.append path must not be used")


def test_large_legacy_response_uses_row_cursor_and_rejects_partial_pages():
    frame = pd.DataFrame({"date": list(range(2001)), "close": 10})
    assert len(_result_frame(Response(frame))) == 2001
    with pytest.raises(ValueError, match="pagination"):
        _result_frame(Response(frame, fail_after=1001))


def fetcher_with_frames(raw):
    hfq = pd.DataFrame({
        "date": ["2024-01-02", "2024-01-03"], "open": [20, 30],
        "high": [20, 30], "low": [20, 30], "close": [20, 30],
        "volume": [9999, 9999], "pctChg": [0, 50],
    })
    responses = iter([Response(hfq), Response(raw)])
    fetcher = BaoStockFetcher.__new__(BaoStockFetcher)
    fetcher._logged_in = False
    fetcher._login = lambda: None
    fetcher._maybe_relogin = lambda: None
    fetcher._bs = SimpleNamespace(query_history_k_data_plus=lambda *a, **kw: next(responses))
    return fetcher


def test_legacy_factor_pairs_dates_and_keeps_raw_odd_share_volume():
    raw = pd.DataFrame({"date": ["2024-01-03", "2024-01-02"], "close": [10, 10], "volume": [103, 101]})
    data = fetcher_with_frames(raw).fetch_daily("510300", "2024-01-02", "2024-01-03")
    assert data["volume"].tolist() == [101, 103]
    assert data["factor"].tolist() == [2, 3]


def test_equal_length_different_dates_cannot_silently_become_factor_one():
    raw = pd.DataFrame({"date": ["2024-01-03", "2024-01-04"], "close": [10, 10], "volume": [103, 101]})
    with pytest.raises(ValueError, match="cannot infer"):
        fetcher_with_frames(raw).fetch_daily("510300", "2024-01-02", "2024-01-03")
