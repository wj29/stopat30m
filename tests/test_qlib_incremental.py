"""Offline float32 Qlib seam/version tests; no provider requests."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from stopat30m.data.qlib_incremental import (
    BridgeError, BridgePolicy, CanonicalIncrement, audit_bridge,
    publish_increment, read_qlib_security, from_baostock_probe,
)

SYMBOL = "SH600000"
FIELDS = ("open", "high", "low", "close", "volume", "factor", "vwap")


def write_bin(path, values, start=0):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(np.asarray([start, *values], dtype="<f4").tobytes())


def make_fixture(tmp_path, *, volume_multiplier=1):
    # These dates are synthetic test fixtures, not a market-calendar adapter.
    days = pd.bdate_range("2024-01-02", periods=45).strftime("%Y-%m-%d").tolist()
    natural = pd.date_range(days[10], days[36]).strftime("%Y-%m-%d").tolist()
    rows = pd.DataFrame({"date": natural, "is_trading_day": [int(day in days) for day in natural]})
    t = np.arange(45)
    raw = pd.DataFrame({"date": days, "instrument": SYMBOL, "open": 10 + t * .02,
        "high": 10.3 + t * .02, "low": 9.8 + t * .02, "close": 10.1 + t * .02,
        "volume": 100000 + t * 100, "tradestatus": "1"})
    raw["amount"] = raw.volume * (raw.open + raw.close) / 2
    factors = np.where(t < 20, 2.0, 2.2)
    adjusted = raw.copy()
    for name in ("open", "high", "low", "close"):
        adjusted[name] *= factors
    scale = 1 / (raw.close.iloc[0] * factors[0])
    base = tmp_path / "immutable-base"
    (base / "calendars").mkdir(parents=True)
    (base / "calendars/day.txt").write_text("\n".join(days) + "\n")
    (base / "instruments").mkdir()
    (base / "instruments/all.txt").write_text(f"{SYMBOL}\t{days[0]}\t{days[29]}\n")
    (base / "instruments/csi300.txt").write_text(f"{SYMBOL}\t{days[3]}\t{days[25]}\n")
    normalized = pd.DataFrame(index=range(30))
    for name in ("open", "high", "low", "close"):
        normalized[name] = adjusted[name].iloc[:30] * scale
    normalized["factor"] = factors[:30] * scale
    normalized["volume"] = raw.volume.iloc[:30] / (normalized.factor * volume_multiplier)
    normalized["vwap"] = raw.amount.iloc[:30] / raw.volume.iloc[:30] * normalized.factor
    for name in FIELDS:
        write_bin(base / "features" / SYMBOL.lower() / (name + ".day.bin"), normalized[name])
    batch = CanonicalIncrement.from_frames(raw.iloc[10:35], adjusted.iloc[10:35], source="synthetic_fixture",
        generation="one-generation", calendar_rows=rows, calendar_source="synthetic_test_flags",
        calendar_source_url="fixture://calendar", requested_start=days[10], requested_end=days[36],
        evidence={"source_url": "fixture://source", "response_sha256": "fixture-only"})
    return base, batch, days, scale


def hashes(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob("*") if path.is_file()}


def test_all_fields_bridge_one_stock_scale_and_watermark_uses_actual_finite_bars(tmp_path):
    base, batch, days, scale = make_fixture(tmp_path)
    result = audit_bridge(base, batch)
    stock = result["securities"][SYMBOL]
    assert stock["normalization_scale"] == pytest.approx(scale, rel=1e-6)
    assert stock["overlap_valid_sessions"] == 20
    assert set(stock["field_checks"]) == set(FIELDS)
    assert stock["before"]["last_complete_finite"] == days[29]
    assert stock["after"]["last_complete_finite"] == result["audited_common_finite_end"] == days[34]
    assert stock["missing_tail_sessions"] == days[35:37]
    assert result["calendar_tail_not_watermark"] == days[44]
    assert stock["new_rows"] == stock["new_finite_rows"] == 5


def test_pilot_publication_keeps_base_bytes_and_old_history_and_does_not_manufacture_index_pool(tmp_path):
    base, batch, days, _ = make_fixture(tmp_path)
    before = hashes(base)
    target = tmp_path / "new-version"
    manifest = publish_increment(base, batch, target)
    assert hashes(base) == before
    assert manifest["scope"] == "fixed_retrospective_pilot_subset"
    assert not (target / "instruments/csi300.txt").exists()
    assert (target / "incremental_manifest.json").exists()
    frame = read_qlib_security(target, SYMBOL)
    assert frame.loc[days[34]].notna().all() and frame.loc[days[35]:].isna().all().all()
    original = read_qlib_security(base, SYMBOL)
    pd.testing.assert_frame_equal(frame.loc[:days[29]], original.loc[:days[29]])
    for name, digest in manifest["dataset_files"].items():
        assert hashlib.sha256((target / name).read_bytes()).hexdigest() == digest
    with pytest.raises(FileExistsError):
        publish_increment(base, batch, target)


def test_full_clone_preserves_original_historical_membership_and_unupdated_fields(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path)
    write_bin(base / "features" / SYMBOL.lower() / "change.day.bin", [np.nan, .01, .02])
    target = tmp_path / "clone"
    old_index = (base / "instruments/csi300.txt").read_bytes()
    manifest = publish_increment(base, batch, target, pilot_only=False)
    assert (target / "instruments/csi300.txt").read_bytes() == old_index
    assert (target / "features" / SYMBOL.lower() / "change.day.bin").read_bytes() == (base / "features" / SYMBOL.lower() / "change.day.bin").read_bytes()
    assert manifest["not_a_full_market_update"]
    assert manifest["securities"][SYMBOL]["nonupdated_extra_features"] == ["change"]


def test_incremental_parent_manifest_is_replaced_without_corrupting_child_inventory(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path)
    first = tmp_path / "first"
    publish_increment(base, batch, first, pilot_only=False)
    before = hashes(first)
    second = tmp_path / "second"
    manifest = publish_increment(first, batch, second, pilot_only=False)
    assert "incremental_manifest.json" not in manifest["dataset_files"]
    for name, digest in manifest["dataset_files"].items():
        assert hashlib.sha256((second / name).read_bytes()).hexdigest() == digest
    assert hashes(first) == before


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume", "factor", "vwap"])
def test_any_required_overlap_field_mismatch_blocks_before_publication(tmp_path, field):
    base, batch, _, _ = make_fixture(tmp_path)
    path = base / "features" / SYMBOL.lower() / (field + ".day.bin")
    values = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
    values[16] *= 1.1
    path.write_bytes(values.tobytes())
    with pytest.raises(BridgeError, match="mismatch"):
        publish_increment(base, batch, tmp_path / "must-not-exist")
    assert not (tmp_path / "must-not-exist").exists()


def test_volume_lot_multiplier_is_explicit_never_inferred_to_make_overlap_pass(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path, volume_multiplier=100)
    with pytest.raises(BridgeError, match="volume"):
        audit_bridge(base, batch)
    assert audit_bridge(base, batch, BridgePolicy(base_raw_volume_multiplier=100))["status"] == "bridge_verified"


def test_factor_one_fallback_and_affine_ohlc_are_rejected(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path)
    frame = batch.bars.copy()
    frame["factor"] = 1
    with pytest.raises(BridgeError, match="multiplicative"):
        audit_bridge(base, replace(batch, bars=frame))
    frame = batch.bars.copy()
    for name in ("open", "high", "low", "close"):
        frame["adjusted_" + name] += 3
    frame["factor"] = frame.adjusted_close / frame.raw_close
    with pytest.raises(BridgeError, match="multiplicative"):
        audit_bridge(base, replace(batch, bars=frame))


def test_raw_and_adjusted_must_align_by_date_not_equal_row_count(tmp_path):
    _, batch, _, _ = make_fixture(tmp_path)
    raw = batch.bars[["date", "instrument", "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume"]].rename(columns=lambda key: key.removeprefix("raw_"))
    adjusted = raw.copy()
    adjusted.loc[adjusted.index[0], "date"] = "2024-01-01"
    with pytest.raises(BridgeError, match="do not align"):
        CanonicalIncrement.from_frames(raw, adjusted, source="fixture", generation="fixture", calendar_rows=batch.calendar_rows,
            calendar_source=batch.calendar_source, calendar_source_url=batch.calendar_source_url,
            requested_start=batch.requested_start, requested_end=batch.requested_end, evidence={"fixture": True})


def test_minimum_twenty_overlap_and_real_calendar_are_required(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path)
    with pytest.raises(BridgeError, match="Fewer than 20"):
        audit_bridge(base, replace(batch, bars=batch.bars.iloc[1:]))
    with pytest.raises(ValueError, match="Incomplete calendar"):
        audit_bridge(base, replace(batch, calendar_rows=batch.calendar_rows.iloc[1:]))
    with pytest.raises(BridgeError, match="at least 20"):
        BridgePolicy(min_overlap=1)


def test_source_and_baseline_gaps_remain_visible_and_unexplained_extension_gap_is_rejected(tmp_path):
    base, batch, days, _ = make_fixture(tmp_path)
    with pytest.raises(BridgeError, match="internal increment session gap"):
        audit_bridge(base, replace(batch, bars=batch.bars.loc[batch.bars.date.ne(days[32])]))
    for name in FIELDS:
        path = base / "features" / SYMBOL.lower() / (name + ".day.bin")
        values = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
        values[6] = np.nan  # Historical gap before the new source overlap.
        path.write_bytes(values.tobytes())
    result = audit_bridge(base, batch)
    assert result["securities"][SYMBOL]["after"]["internal_gap_dates"] == [days[5]]
    assert result["securities"][SYMBOL]["after"]["gap_classification"].startswith("unknown_or_suspension")


def test_known_suspension_is_not_filled_or_counted_as_finite_watermark(tmp_path):
    base, batch, days, _ = make_fixture(tmp_path)
    bars = batch.bars.copy()
    suspended = bars.date.eq(days[34])
    bars.loc[suspended, "tradestatus"] = "0"
    bars.loc[suspended, [name for name in bars if name.startswith("raw_") or name.startswith("adjusted_") or name == "factor"]] = np.nan
    result = publish_increment(base, replace(batch, bars=bars), tmp_path / "halt-version")
    assert result["audited_common_finite_end"] == days[33]
    assert result["securities"][SYMBOL]["source_observed_last_row"] == days[34]
    assert result["securities"][SYMBOL]["known_suspended_dates"] == [days[34]]


def test_partial_base_tail_and_mixed_source_batches_do_not_silently_advance(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path)
    bars = batch.bars.assign(source="other-source")
    with pytest.raises(BridgeError, match="Mixed-source"):
        audit_bridge(base, replace(batch, bars=bars))
    path = base / "features" / SYMBOL.lower() / "close.day.bin"
    values = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
    path.write_bytes(np.concatenate([values, [values[-1]]]).astype("<f4").tobytes())
    with pytest.raises(BridgeError, match="Partial baseline tail"):
        audit_bridge(base, batch)


def test_invalid_binary_headers_and_existing_base_write_targets_fail(tmp_path):
    base, batch, _, _ = make_fixture(tmp_path)
    with pytest.raises(BridgeError, match="outside and distinct"):
        publish_increment(base, batch, base / "child")
    path = base / "features" / SYMBOL.lower() / "close.day.bin"
    values = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
    values[0] = .5
    path.write_bytes(values.tobytes())
    with pytest.raises(BridgeError, match="header"):
        read_qlib_security(base, SYMBOL)


@pytest.mark.parametrize("corruption", ["unknown_status", "invalid_ohlc", "amount_unit"])
def test_invalid_source_semantics_block_even_when_adjustment_is_multiplicative(tmp_path, corruption):
    base, batch, _, _ = make_fixture(tmp_path)
    bars = batch.bars.copy()
    # Corrupt only a newly appended row; valid overlap must not conceal it.
    index = bars.index[-1]
    if corruption == "unknown_status":
        bars.loc[index, "tradestatus"] = "unknown"
    elif corruption == "invalid_ohlc":
        bars.loc[index, "raw_high"] = bars.loc[index, "raw_low"]
        bars.loc[index, "adjusted_high"] = bars.loc[index, "raw_high"] * bars.loc[index, "factor"]
    else:
        bars.loc[index, "raw_amount"] /= 1000
    with pytest.raises(BridgeError, match="trading status|high/low|VWAP lies outside"):
        publish_increment(base, replace(batch, bars=bars), tmp_path / "blocked")
    assert not (tmp_path / "blocked").exists()


def test_explicit_volume_unit_does_not_accept_boolean():
    with pytest.raises(BridgeError, match="volume units"):
        BridgePolicy(base_raw_volume_multiplier=True)


def test_saved_baostock_probe_receipts_modes_and_hashes_are_verified_offline(tmp_path):
    _, batch, _, _ = make_fixture(tmp_path)
    probe = tmp_path / "probe"
    probe.mkdir()
    responses = []
    for mode, prefix, flag in (("raw", "raw", "3"), ("hfq", "adjusted", "1")):
        frame = batch.bars[["date", "instrument", *[prefix + "_" + name for name in ("open", "high", "low", "close")]]].copy()
        frame = frame.rename(columns={"instrument": "code", **{prefix + "_" + name: name for name in ("open", "high", "low", "close")}})
        frame["code"] = "sh.600000"
        frame["adjustflag"] = flag
        frame["volume"] = batch.bars.raw_volume
        frame["amount"] = batch.bars.raw_amount
        frame["tradestatus"] = "1"
        payload = {"request": {"adjustflag": flag}, "fields": frame.columns.tolist(), "rows": frame.values.tolist()}
        path = probe / (mode + ".json")
        path.write_text(json.dumps(payload))
        responses.append({"mode": mode, "file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    receipt = {"source": "baostock", "instrument": SYMBOL, "generation": "captured-generation",
        "request_start": batch.requested_start, "request_end": batch.requested_end,
        "status": "complete", "responses": responses}
    (probe / "receipt.json").write_text(json.dumps(receipt))
    options = {"calendar_rows": batch.calendar_rows, "calendar_source": batch.calendar_source,
        "calendar_source_url": batch.calendar_source_url}
    loaded = from_baostock_probe(probe, **options)
    assert loaded.source == "baostock" and loaded.generation == "captured-generation"
    pd.testing.assert_series_equal(loaded.bars.factor, batch.bars.factor)
    # A modified response cannot be accepted merely because it parses as JSON.
    (probe / "raw.json").write_text((probe / "raw.json").read_text() + " ")
    with pytest.raises(BridgeError, match="checksum"):
        from_baostock_probe(probe, **options)
