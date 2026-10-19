"""Whole-security replacement tests use tiny synthetic histories, never returns."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import zipfile

import numpy as np
import pandas as pd
import pytest

from stopat30m.data.qlib_incremental import CanonicalIncrement, read_qlib_security
from stopat30m.data.qlib_rebuild import (publish_whole_security_rebuild, RebuildError, MODE,
                                       _validated_subset_receipt)
from stopat30m.data.qlib_snapshot import prepare_qlib_snapshot, prepare_qlib_directory, load_qlib_snapshot

A, B = "SH600018", "SZ000001"
OLD = ["2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"]
DAYS = [*OLD, "2026-10-19"]
SOURCE = {"kind": "community", "release_tag": "synthetic", "url": "https://example.test/fixture.zip"}


def bin_values(values):
    return np.asarray([0, *values], dtype="<f4").tobytes()


@pytest.fixture
def case(tmp_path):
    archive = tmp_path / "base.zip"
    with zipfile.ZipFile(archive, "w") as stream:
        stream.writestr("qlib/calendars/day.txt", "\n".join(OLD) + "\n")
        stream.writestr("qlib/calendars/day_future.txt", "\n".join([*DAYS, "2026-10-20"]) + "\n")
        members = f"{A}\t{OLD[0]}\t{OLD[-1]}\n{B}\t{OLD[0]}\t{OLD[-1]}\n"
        stream.writestr("qlib/instruments/all.txt", members)
        stream.writestr("qlib/instruments/csi300.txt", members)
        for inst in (A, B):
            for field in ("open", "high", "low", "close", "volume", "factor", "vwap", "adjclose", "amount", "change"):
                value = {"high": 9.1, "low": 8.9, "volume": 100, "factor": .5}.get(field, 9)
                stream.writestr(f"qlib/features/{inst.lower()}/{field}.day.bin", bin_values([value] * len(OLD)))
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    parent = prepare_qlib_snapshot(archive, tmp_path / "parent", source=SOURCE, expected_sha256=sha)
    raw, adj = [], []
    for offset, day in enumerate(DAYS):
        price = 10 + offset
        row = {"date": day, "instrument": A, "open": price, "high": price,
               "low": price, "close": price, "volume": 1000, "amount": price * 1000, "tradestatus": "1"}
        raw.append(row)
        adj.append({**row, **{field: price * 2 for field in ("open", "high", "low", "close")}})
    calendar = pd.DataFrame([{"date": str(day.date()), "is_open": str(day.date()) in DAYS}
                             for day in pd.date_range(DAYS[0], DAYS[-1])])
    batch = CanonicalIncrement.from_frames(pd.DataFrame(raw), pd.DataFrame(adj), source="baostock",
        generation="test-source-generation", calendar_rows=calendar, calendar_source="fixture:explicit-sessions",
        calendar_source_url="fixture:calendar", requested_start=DAYS[0], requested_end=DAYS[-1],
        evidence={"synthetic": True, "original_response_sha256": "a" * 64})
    failure = tmp_path / "failed_seams.json"
    failure.write_text(json.dumps({"failed_instruments": [A], "securities": {A: {"status": "failed"}}}))
    return {"parent": parent, "batch": batch, "failure": failure, "target": tmp_path / "rebuilt"}


def publish(case):
    return publish_whole_security_rebuild(case["parent"].dataset_dir, case["batch"], case["target"],
                                           failure_audit_path=case["failure"])


def test_whole_history_replacement_uses_one_normalization_and_explicitly_deletes_old_extra_fields(case):
    parent = case["parent"]
    original = {str(path.relative_to(parent.dataset_dir)): path.read_bytes()
                for path in parent.dataset_dir.rglob("*") if path.is_file()}
    manifest = publish(case)
    assert manifest["status"] == MODE and manifest["mode"] == MODE
    assert manifest["policy"]["cross_source_seam_claimed"] is False
    assert manifest["selection"]["failed_instruments"] == [A]
    assert set(manifest["securities"][A]["deleted_extra_features"]) == {
        "adjclose.day.bin", "amount.day.bin", "change.day.bin"}
    actual = read_qlib_security(case["target"], A)
    assert actual.close.iloc[0] == 1
    assert actual.close.iloc[-1] == pytest.approx(1.5)
    for offset, (_, row) in enumerate(actual.iterrows()):
        assert row.close / row.factor == pytest.approx(10 + offset)
        assert row.volume * row.factor * 100 == pytest.approx(1000)
        assert row.vwap / row.factor == pytest.approx(10 + offset)
    assert set(path.name for path in (case["target"] / "features" / A.lower()).iterdir()) == {
        field + ".day.bin" for field in ("open", "high", "low", "close", "volume", "factor", "vwap")}
    for name, raw in original.items():
        assert (parent.dataset_dir / name).read_bytes() == raw
        if not name.startswith("features/" + A.lower()) and not name.startswith("calendars/") and name != "instruments/all.txt":
            assert (case["target"] / name).read_bytes() == raw
    child = prepare_qlib_directory(case["target"], case["target"].parent / "versions", source=SOURCE,
        parent_snapshot=parent, increment_manifest_path=case["target"] / "incremental_manifest.json")
    assert child.manifest["lineage"]["increment"]["status"] == MODE
    assert child.manifest["lineage"]["increment"]["mode"] == MODE
    assert child.manifest["kind"] == "qlib_cn_incremental_snapshot" and "archive" not in child.manifest
    assert load_qlib_snapshot(child.path).manifest_sha256 == child.manifest_sha256
    with pytest.raises(RebuildError, match="new directory"):
        publish(case)


def test_suspended_missing_and_incomplete_quotes_remain_nan_with_distinct_source_status(case):
    batch = case["batch"]
    bars = batch.bars.copy()
    bars.loc[bars.date.eq(DAYS[2]), "tradestatus"] = "0"
    bars = bars.loc[~bars.date.eq(DAYS[3])].copy()
    bars.loc[bars.date.eq(DAYS[4]), "raw_amount"] = 0
    case["batch"] = replace(batch, bars=bars)
    manifest = publish(case)
    actual = read_qlib_security(case["target"], A)
    assert actual.loc[DAYS[2]:DAYS[4]].isna().all().all()
    assert actual.loc[DAYS[-1], "close"] == pytest.approx(1.5)
    statuses = json.loads((case["target"] / "rebuild_status.json").read_text())[A]
    assert statuses[DAYS[2]] == "source_suspended"
    assert statuses[DAYS[3]] == "source_missing_bar"
    assert statuses[DAYS[4]] == "source_incomplete_quote"
    report = manifest["securities"][A]
    assert report["suspended_dates"] == [DAYS[2]]
    assert report["unknown_missing_dates"] == [DAYS[3], DAYS[4]]
    assert report["parent_finite_sessions_now_unavailable"] == DAYS[2:5]
    assert report["parent_finite_sessions_now_unavailable_count"] == 3
    assert report["whole_history_equivalent_to_parent_claimed"] is False


def test_aligned_truncated_histories_cannot_discard_the_parents_newer_finite_bars(case):
    batch = case["batch"]
    case["batch"] = replace(batch, bars=batch.bars.iloc[:3].copy())
    with pytest.raises(RebuildError, match="newer finite history"):
        publish(case)
    assert not case["target"].exists()


@pytest.mark.parametrize("defect", ["source", "selection", "fractional_volume", "bad_amount_units",
    "affine_ohlc", "invalid_ohlc", "infinite", "unknown_status", "earlier_history_missing", "missing_amount"])
def test_rebuild_refuses_unproven_sources_units_adjustment_selection_and_lost_history(case, defect):
    batch = case["batch"]
    bars = batch.bars.copy()
    number_fields = bars.select_dtypes(include="number").columns
    bars[number_fields] = bars[number_fields].astype(float)
    if defect == "source":
        batch = replace(batch, source="other_provider")
    elif defect == "selection":
        case["failure"].write_text(json.dumps({"failed_instruments": [B]}))
    elif defect == "fractional_volume":
        bars.loc[0, "raw_volume"] = 1000.5
    elif defect == "bad_amount_units":
        bars.loc[0, "raw_amount"] *= 1000
    elif defect == "affine_ohlc":
        bars.loc[0, "adjusted_high"] = bars.loc[0, "adjusted_close"] * 1.01
    elif defect == "invalid_ohlc":
        bars.loc[0, "raw_high"] = 1
    elif defect == "infinite":
        bars.loc[0, "raw_open"] = np.inf
    elif defect == "unknown_status":
        bars.loc[0, "tradestatus"] = "unknown"
    elif defect == "earlier_history_missing":
        bars = bars.iloc[1:].copy()
    else:
        bars = bars.drop(columns="raw_amount")
    case["batch"] = replace(batch, bars=bars)
    with pytest.raises(RebuildError):
        publish(case)
    assert not case["target"].exists()


def test_a_truncated_raw_history_cannot_be_paired_by_row_count_or_silently_cut_to_common_dates(case):
    batch = case["batch"]
    raw = batch.bars[["date", "instrument", "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "raw_amount", "tradestatus"]].rename(
        columns={"raw_" + field: field for field in ("open", "high", "low", "close", "volume", "amount")})
    adjusted = batch.bars[["date", "instrument", "adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close"]].rename(
        columns={"adjusted_" + field: field for field in ("open", "high", "low", "close")})
    with pytest.raises(ValueError, match="do not align"):
        CanonicalIncrement.from_frames(raw.iloc[:-1], adjusted, source=batch.source, generation=batch.generation,
            calendar_rows=batch.calendar_rows, calendar_source=batch.calendar_source,
            calendar_source_url=batch.calendar_source_url, requested_start=batch.requested_start,
            requested_end=batch.requested_end, evidence=batch.evidence)
    assert not case["target"].exists()


@pytest.mark.parametrize("defect", [None, "generation", "response_time", "excluded_selection", "response_bytes"])
def test_subset_preserves_original_response_bytes_generation_times_and_exclusion_evidence(tmp_path, defect):
    original_root, view = tmp_path / "original", tmp_path / "subset"
    original_root.mkdir(); view.mkdir()
    payload = b'{"source":"synthetic original response"}'
    record = {"file": "SH600018_raw.json", "sha256": hashlib.sha256(payload).hexdigest(),
              "fetched_at": "2026-10-19T09:00:00+00:00"}
    (original_root / record["file"]).write_bytes(payload)
    (view / record["file"]).write_bytes(payload)
    original = {"generation": "original-generation", "instruments": [A, B], "responses": {A + "_raw": record}}
    original_raw = json.dumps(original).encode(); (original_root / "collection.json").write_bytes(original_raw)
    receipt = {"kind": "validated_source_subset_view", "original_collection": str(original_root),
        "original_collection_manifest_sha256": hashlib.sha256(original_raw).hexdigest(),
        "original_generation_preserved": original["generation"], "selected_instruments": [A],
        "excluded_instruments": {B: {"reason": "source identity failed", "failure_sha256": "a" * 64}},
        "new_source_generation_claimed": False, "raw_response_bytes_unmodified": True,
        "original_response_fetched_at_preserved": True}
    collection = {"generation": original["generation"], "instruments": [A],
                  "responses": {A + "_raw": dict(record)}, "derived_view": receipt}
    if defect == "generation":
        collection["generation"] = "newly-invented-generation"
    elif defect == "response_time":
        collection["responses"][A + "_raw"]["fetched_at"] = "2026-10-19T10:00:00+00:00"
    elif defect == "excluded_selection":
        receipt["excluded_instruments"] = {}
    elif defect == "response_bytes":
        (view / record["file"]).write_bytes(b"modified")
    (view / "subset_receipt.json").write_text(json.dumps(receipt))
    if defect:
        with pytest.raises(RebuildError):
            _validated_subset_receipt(view, collection)
    else:
        proof = _validated_subset_receipt(view, collection)
        assert proof["original_generation_preserved"] == original["generation"]
        assert proof["excluded_instruments"] == receipt["excluded_instruments"]
