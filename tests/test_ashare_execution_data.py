from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from stopat30m.data.ashare_daily import _save
from stopat30m.data.ashare_execution_data import (EXECUTION_FIELDS, collect_execution_data,
    load_execution_collection, historical_download_targets)


class Response:
    error_code = "0"
    def __init__(self, fields, rows):
        self.fields, self.rows, self.index = fields, rows, -1
    def next(self):
        self.index += 1
        return self.index < len(self.rows)
    def get_row_data(self):
        return self.rows[self.index]


class Source:
    def __init__(self):
        self.calls = 0
    def login(self):
        return SimpleNamespace(error_code="0")
    def logout(self):
        pass
    def query_stock_basic(self):
        return Response(["code", "ipoDate", "outDate", "type", "status"],
                        [["sh.600000", "1999-11-10", "", "1", "1"]])
    def query_history_k_data_plus(self, **request):
        self.calls += 1
        return Response(EXECUTION_FIELDS.split(","), [["2026-10-19", "sh.600000", "10", "11", "9", "10.5", "8.5", "1000", "10200", "3", "1", "1"]])


@pytest.fixture
def capture(tmp_path, monkeypatch):
    monkeypatch.setattr("stopat30m.data.ashare_execution_data.time.sleep", lambda _: None)
    selection = tmp_path / "csi300.txt"
    selection.write_text("SH600000\t2024-01-01\t2026-10-19\n")
    source, root = Source(), tmp_path / "originals"
    kwargs = {"instruments": ["SH600000"], "selection_evidence_path": selection, "client": source}
    collect_execution_data(root, "2024-12-31", "2026-10-19", **kwargs)
    return root, source, kwargs


def test_execution_preclose_and_daily_st_preserved_and_complete_resume_does_not_refetch(capture):
    root, source, kwargs = capture
    frame, basic, receipt = load_execution_collection(root)
    assert frame.preclose.iloc[0] == "8.5" and frame.close.iloc[0] == "10.5"
    assert frame.isST.iloc[0] == "1" and frame.tradestatus.iloc[0] == "1"
    assert basic.ipoDate.iloc[0] == "1999-11-10"
    assert receipt["live_opening_quote"] is False and receipt["units"]["volume"] == "shares"
    previous = (root / "SH600000.json").read_bytes()
    collect_execution_data(root, "2024-12-31", "2026-10-19", **kwargs)
    assert source.calls == 1 and (root / "SH600000.json").read_bytes() == previous


@pytest.mark.parametrize("defect", ["status", "st", "mode", "volume", "preclose", "date", "code", "checksum"])
def test_execution_loader_rejects_semantic_bad_source_even_with_updated_outer_checksum(capture, defect):
    root, _, _ = capture
    path = root / "SH600000.json"
    payload = json.loads(path.read_bytes())
    index, value = {"status": (10, ""), "st": (11, "?"), "mode": (9, "1"), "volume": (7, "1.5"),
                    "preclose": (6, "inf"), "date": (0, "2024-01-01"), "code": (1, "sh.600001"), "checksum": (6, "8")}[defect]
    payload["rows"][0][index] = value
    digest = _save(path, payload)
    if defect != "checksum":
        manifest = json.loads((root / "execution_collection.json").read_bytes())
        manifest["responses"]["SH600000"]["sha256"] = digest
        _save(root / "execution_collection.json", manifest)
    with pytest.raises(ValueError):
        load_execution_collection(root)


def test_execution_selection_changed_is_not_silently_reused(capture):
    root, _, kwargs = capture
    kwargs["selection_evidence_path"].write_text("changed historical membership\n")
    with pytest.raises(ValueError, match="checksum"):
        load_execution_collection(root)


def test_historical_targets_include_removed_members_and_preserve_old_alias_outside_period(tmp_path):
    path = tmp_path / "csi300.txt"
    path.write_text("SHT00018\t2005-01-01\t2006-12-31\nSH600000\t2024-01-01\t2025-03-01\n"
                    "SZ000001\t2025-06-01\t2026-10-19\nSH600001\t2020-01-01\t2024-12-30\n")
    assert historical_download_targets(path, "2024-12-31", "2026-10-19") == ["SH600000", "SZ000001"]
    with pytest.raises(ValueError, match="unsupported"):
        historical_download_targets(path, "2005-01-01", "2006-12-31")
