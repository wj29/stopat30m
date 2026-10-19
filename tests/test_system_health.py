import json
import struct
from datetime import date

from stopat30m.data.health import inspect_data


def make_dataset(path, stocks=None):
    (path / "calendars").mkdir(parents=True)
    (path / "calendars/day.txt").write_text("2026-10-15\n2026-10-16\n")
    (path / "instruments").mkdir()
    (path / "instruments/all.txt").write_text("SH600000\t2026-10-15\t2026-10-16\n")
    (path / "features/sh600000").mkdir(parents=True)
    (path / "features/sh600000/close.day.bin").write_bytes(struct.pack("ff", 0, 10))
    if stocks is not None:
        (path / "data_meta.json").write_text(json.dumps({"stocks": stocks, "trusted_until": "2026-10-16"}))


def test_empty_directories_are_not_available(tmp_path):
    for name in ("calendars", "features", "instruments"):
        (tmp_path / name).mkdir()
    result = inspect_data(tmp_path, today=date(2026, 10, 19))
    assert result["data_available"] is False
    assert result["status"] == "missing"
    assert result["coverage_pct"] is None


def test_partial_coverage_and_missing_dates_are_visible(tmp_path):
    make_dataset(tmp_path, {
        "SH600000": {"data_end": "2026-10-16", "ipo_date": "2000-01-01"},
        "SH600001": {"data_end": "2026-10-15", "ipo_date": "2000-01-01"},
        "SH600002": {"data_end": None},
    })
    result = inspect_data(tmp_path, today=date(2026, 10, 19))
    assert result["status"] == "degraded"
    assert result["lagging_stock_count"] == 1
    assert result["missing_stock_count"] == 1
    assert result["excluded_stock_count"] == 1
    assert result["coverage_pct"] == 33.33
    assert result["last_data_date"] == "2026-10-16"
    assert result["freshness_confirmed"] is False


def test_old_calendar_does_not_prove_freshness(tmp_path):
    make_dataset(tmp_path, {"SH600000": {"data_end": "2026-10-16", "ipo_date": "2000-01-01"}})
    result = inspect_data(tmp_path, today=date(2026, 11, 19))
    assert result["status"] == "stale"
    assert result["age_calendar_days"] == 34
    assert not result["freshness_confirmed"]


def test_corrupt_metadata_is_reported(tmp_path):
    make_dataset(tmp_path)
    (tmp_path / "data_meta.json").write_text("not json")
    result = inspect_data(tmp_path, today=date(2026, 10, 19))
    assert result["status"] == "degraded"
    assert any("损坏" in warning for warning in result["warnings"])
