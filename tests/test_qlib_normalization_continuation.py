"""Offline parent-scale continuation proofs; original seam tolerances stay fixed.

Every parent here is produced by the real publisher.  No provider, live paper
directory, clock, historical model, or network is used by these tests.
"""
from dataclasses import asdict, replace
import hashlib
import json
import shutil

import numpy as np
import pandas as pd
import pytest

from stopat30m.data import qlib_continuation as q
from stopat30m.data import qlib_incremental as original
from test_qlib_incremental import FIELDS, SYMBOL, hashes, make_fixture, write_bin


OTHER_SYMBOL = "SZ300999"


def _calendar_rows(days, first, last):
    natural = pd.date_range(days[first], days[last]).strftime("%Y-%m-%d").tolist()
    return pd.DataFrame({"date": natural, "is_trading_day": [int(day in days) for day in natural]})


def _boundary_inputs(tmp_path, *, second_security=False, suspended_days=()):
    """A close-median change of one float32 ULP crosses the factor boundary.

    The initial 30 sessions have 16 high / 14 low close ratios, giving the
    upper median.  The next 20 have 6 high / 14 low, giving 1.  Source values
    on the 20 common sessions are identical.  The parent's verified constant
    passes all seven fields with the original rtol and absolute tolerance.
    """
    base, _, days, _ = make_fixture(tmp_path)
    scale = float(np.nextafter(np.float32(1), np.float32(2)))
    raw = pd.DataFrame({
        "date": days, "instrument": SYMBOL, "open": 1., "high": 1.1,
        "low": .9, "close": 1., "volume": 100000., "amount": 100000.,
        "tradestatus": "1",
    })
    adjusted = raw.copy()
    baseline = {
        "open": np.full(30, scale), "high": np.full(30, 1.1 * scale),
        "low": np.full(30, .9 * scale), "close": np.full(30, scale),
        "factor": np.full(30, float(np.float32(1.0005002))),
        "volume": np.full(30, 100000. / scale), "vwap": np.full(30, scale),
    }
    baseline["close"][15:29] = 1.
    for index in suspended_days:
        raw.loc[index, "tradestatus"] = "0"
        raw.loc[index, ["open", "high", "low", "close", "volume", "amount"]] = np.nan
        adjusted.loc[index, ["open", "high", "low", "close"]] = np.nan
        for values in baseline.values():
            values[index] = np.nan
    for field, values in baseline.items():
        write_bin(base / "features" / SYMBOL.lower() / f"{field}.day.bin", values)

    if second_security:
        shutil.copytree(base / "features" / SYMBOL.lower(), base / "features" / OTHER_SYMBOL.lower())
        for filename in ("all.txt", "csi300.txt"):
            path = base / "instruments" / filename
            path.write_text(path.read_text() + f"{OTHER_SYMBOL}\t{days[0]}\t{days[29]}\n")
        raw = pd.concat([raw, raw.assign(instrument=OTHER_SYMBOL)], ignore_index=True)
        adjusted = pd.concat([adjusted, adjusted.assign(instrument=OTHER_SYMBOL)], ignore_index=True)

    def batch(first, last, generation):
        mask = raw.date.between(days[first], days[last])
        adjusted_mask = adjusted.date.between(days[first], days[last])
        return q.CanonicalIncrement.from_frames(
            raw.loc[mask], adjusted.loc[adjusted_mask], source="synthetic_fixture",
            generation=generation, calendar_rows=_calendar_rows(days, first, last),
            calendar_source="synthetic_test_flags", calendar_source_url="fixture://calendar",
            requested_start=days[first], requested_end=days[last],
            evidence={"source_url": "fixture://source", "response_sha256": generation},
        )

    return base, batch(0, 34, "parent-generation"), batch(15, 39, "child-generation"), days, scale


def _published_parent(tmp_path, **kwargs):
    base, previous, current, days, scale = _boundary_inputs(tmp_path, **kwargs)
    parent = tmp_path / "verified-parent"
    manifest = q.publish_increment(base, previous, parent, pilot_only=False)
    proof = hashlib.sha256((parent / "incremental_manifest.json").read_bytes()).hexdigest()
    continuation = q.NormalizationContinuation(previous_batch=previous, parent_increment_sha256=proof)
    return base, parent, previous, current, days, scale, manifest, continuation


def _replace_current_rows(batch, *, first=None, last=None):
    bars = batch.bars
    if first is not None:
        bars = bars.loc[bars.date.ge(first)]
    if last is not None:
        bars = bars.loc[bars.date.le(last)]
    return replace(batch, bars=bars.copy())


def _reseal_parent(parent, manifest, continuation):
    """Give intentionally inconsistent metadata a valid outer and inner SHA."""
    unsigned = {key: value for key, value in manifest.items() if key != "version_sha256"}
    canonical = json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    manifest["version_sha256"] = hashlib.sha256(canonical).hexdigest()
    path = parent / "incremental_manifest.json"
    path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
    return replace(continuation, parent_increment_sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def test_verified_parent_constant_passes_where_new_median_alone_fails(tmp_path):
    _, parent, previous, current, days, scale, manifest, continuation = _published_parent(tmp_path)
    policy = q.BridgePolicy()
    assert manifest["securities"][SYMBOL]["normalization_scale"] == scale
    old = q.read_qlib_security(parent, SYMBOL)
    common = current.bars.date[current.bars.date.le(days[34])].tolist()
    assert len(common) == 20
    assert float(np.median(old.loc[common, "close"] / current.bars.set_index("date").loc[common, "adjusted_close"])) == 1.
    pd.testing.assert_frame_equal(
        previous.bars.set_index(["date", "instrument"]).loc[pd.IndexSlice[days[15]:days[34], :], :],
        current.bars.set_index(["date", "instrument"]).loc[pd.IndexSlice[days[15]:days[34], :], :],
    )
    # No continuation keeps the initial-median algorithm, even though a
    # previously verified constant could pass.  There is no scale search.
    with pytest.raises(q.BridgeError, match="factor"):
        q.audit_bridge(parent, current)
    report = q.audit_bridge(parent, current, continuation=continuation)
    stock = report["securities"][SYMBOL]
    assert stock["normalization_scale"] == scale
    assert stock["normalization_method"] == "fixed_verified_parent_scale"
    assert stock["overlap_valid_sessions"] == 20
    assert set(stock["field_checks"]) == set(FIELDS)
    assert stock["field_checks"]["factor"]["rtol"] == policy.factor_rtol == .0005
    assert stock["field_checks"]["factor"]["max_relative_error"] > .0005
    # The unchanged absolute 1e-7 allowance is material at this exact boundary.
    assert stock["new_finite_rows"] == 5
    assert stock["after"]["last_complete_finite"] == days[39]
    provenance = report["normalization_continuation"]
    assert provenance["kind"] == "verified_parent_source_normalization_continuation"
    assert provenance["parent_increment_manifest"] == {
        "path": str((parent / "incremental_manifest.json").resolve()),
        "sha256": continuation.parent_increment_sha256,
    }
    assert provenance["unchanged_common_source_sessions"] == {SYMBOL: 20}
    assert provenance["previous_source"] == previous.source == current.source
    assert provenance["previous_generation"] == previous.generation
    assert provenance["current_generation"] == current.generation
    assert provenance["previous_source_bars_sha256"] == manifest["source_bars_sha256"]
    assert provenance["historical_source_revisions_accepted"] is False
    assert provenance["tolerances_relaxed"] is False
    assert provenance["scale_refitted"] is False
    assert report["policy"] == asdict(policy)


def test_first_generation_keeps_original_schema_and_median_without_parent_evidence(tmp_path):
    base, previous, _, _, scale = _boundary_inputs(tmp_path)
    report = q.audit_bridge(base, previous)
    assert report["securities"][SYMBOL]["normalization_scale"] == scale
    assert "normalization_method" not in report["securities"][SYMBOL]
    assert "normalization_continuation" not in report
    assert report == original.audit_bridge(base, previous)
    assert q.CanonicalIncrement is original.CanonicalIncrement
    assert q.BridgePolicy is original.BridgePolicy
    assert q.BridgeError is original.BridgeError


def test_newly_audited_security_uses_initial_median_and_cannot_inherit_another_scale(tmp_path):
    base, previous, current, days, scale = _boundary_inputs(tmp_path, second_security=True)
    # The full parent clone contains both securities; its source proof has
    # reconciled only SYMBOL.  The other one still needs a first seam audit.
    parent_previous = replace(previous, bars=previous.bars.loc[previous.bars.instrument.eq(SYMBOL)].copy())
    parent = tmp_path / "only-one-security-verified-parent"
    q.publish_increment(base, parent_previous, parent, pilot_only=False)
    continuation = q.NormalizationContinuation(
        previous_batch=parent_previous,
        parent_increment_sha256=hashlib.sha256((parent / "incremental_manifest.json").read_bytes()).hexdigest(),
    )
    earlier_other = previous.bars.loc[previous.bars.instrument.eq(OTHER_SYMBOL) & previous.bars.date.lt(days[15])]
    expanded = replace(
        current, bars=pd.concat([current.bars, earlier_other], ignore_index=True).sort_values(["instrument", "date"]),
        requested_start=days[0], calendar_rows=_calendar_rows(days, 0, 39),
    )
    report = q.audit_bridge(parent, expanded, continuation=continuation)
    assert report["securities"][SYMBOL]["normalization_method"] == "fixed_verified_parent_scale"
    assert report["securities"][OTHER_SYMBOL]["normalization_method"] == "initial_close_median"
    assert report["securities"][OTHER_SYMBOL]["normalization_scale"] == scale
    assert report["securities"][OTHER_SYMBOL]["overlap_valid_sessions"] == 30
    assert report["normalization_continuation"]["unchanged_common_source_sessions"] == {SYMBOL: 20}


@pytest.mark.parametrize("corruption", ["current_source", "previous_evidence", "previous_volume_unit", "current_volume_unit", "volume_policy"])
def test_source_identity_provenance_and_explicit_volume_units_cannot_change(tmp_path, corruption):
    _, parent, previous, current, _, _, _, continuation = _published_parent(tmp_path)
    policy = None
    if corruption == "current_source":
        current = replace(current, source="another-provider")
    elif corruption == "previous_evidence":
        continuation = replace(continuation, previous_batch=replace(previous, evidence={"source_url": "fixture://wrong"}))
    elif corruption == "previous_volume_unit":
        continuation = replace(continuation, previous_batch=replace(previous, volume_unit="lots"))
    elif corruption == "current_volume_unit":
        current = replace(current, volume_unit="lots")
    else:
        policy = q.BridgePolicy(base_raw_volume_multiplier=100)
    with pytest.raises(q.BridgeError):
        q.audit_bridge(parent, current, policy, continuation=continuation)


@pytest.mark.parametrize("field", [
    "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "raw_amount",
    "adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close", "factor", "tradestatus",
])
def test_even_sub_tolerance_common_source_revision_is_rejected(tmp_path, field):
    _, parent, _, current, _, _, _, continuation = _published_parent(tmp_path)
    bars = current.bars.copy()
    index = bars.index[0]
    if field == "tradestatus":
        # Semantically the same traded status still changes the saved source.
        bars.loc[index, field] = "true"
    elif field == "raw_volume":
        bars.loc[index, field] += 1.
    else:
        bars.loc[index, field] = np.nextafter(float(bars.loc[index, field]), np.inf)
    target = tmp_path / "revised-source-must-not-publish"
    before = hashes(parent)
    with pytest.raises(q.BridgeError):
        q.publish_increment(parent, replace(current, bars=bars), target, continuation=continuation)
    assert not target.exists()
    assert hashes(parent) == before


def test_hfq_rebasing_is_not_implicitly_rescaled_to_reuse_parent(tmp_path):
    _, parent, _, current, _, _, _, continuation = _published_parent(tmp_path)
    bars = current.bars.copy()
    bars[[f"adjusted_{field}" for field in q.OHLC] + ["factor"]] *= 2.
    with pytest.raises(q.BridgeError):
        q.audit_bridge(parent, replace(current, bars=bars), continuation=continuation)


@pytest.mark.parametrize("corruption", ["hash", "source", "generation", "policy", "previous_bars", "missing_manifest"])
def test_parent_identity_and_previous_source_proof_are_mandatory(tmp_path, corruption):
    _, parent, previous, current, _, _, _, continuation = _published_parent(tmp_path)
    policy = None
    if corruption == "hash":
        continuation = replace(continuation, parent_increment_sha256="0" * 64)
    elif corruption == "source":
        continuation = replace(continuation, previous_batch=replace(previous, source="other-source"))
    elif corruption == "generation":
        continuation = replace(continuation, previous_batch=replace(previous, generation="other-generation"))
    elif corruption == "policy":
        policy = q.BridgePolicy(price_rtol=.0006)
    elif corruption == "previous_bars":
        bars = previous.bars.copy()
        bars.loc[bars.index[0], "raw_amount"] += .01
        continuation = replace(continuation, previous_batch=replace(previous, bars=bars))
    else:
        (parent / "incremental_manifest.json").unlink()
    before = hashes(parent)
    target = tmp_path / "unproved-parent-must-not-publish"
    with pytest.raises(q.BridgeError):
        q.publish_increment(parent, current, target, policy, continuation=continuation)
    assert not target.exists()
    assert hashes(parent) == before


@pytest.mark.parametrize("key", ["source", "generation", "source_bars_sha256", "policy"])
def test_matching_outer_sha_does_not_make_inconsistent_parent_metadata_trusted(tmp_path, key):
    _, parent, _, current, _, _, _, continuation = _published_parent(tmp_path)
    path = parent / "incremental_manifest.json"
    manifest = json.loads(path.read_text())
    if key == "policy":
        manifest[key]["price_rtol"] = .0006
    else:
        manifest[key] = "incorrect-parent-evidence"
    continuation = _reseal_parent(parent, manifest, continuation)
    with pytest.raises(q.BridgeError):
        q.audit_bridge(parent, current, continuation=continuation)


@pytest.mark.parametrize("scale", [True, 0., -1., "1.0"])
def test_valid_sha_cannot_authorize_invalid_parent_scale_values(tmp_path, scale):
    _, parent, _, current, _, _, _, continuation = _published_parent(tmp_path)
    manifest = json.loads((parent / "incremental_manifest.json").read_text())
    manifest["securities"][SYMBOL]["normalization_scale"] = scale
    continuation = _reseal_parent(parent, manifest, continuation)
    with pytest.raises(q.BridgeError, match="scale"):
        q.audit_bridge(parent, current, continuation=continuation)


@pytest.mark.parametrize("first", [16, 35])
def test_fewer_than_twenty_or_no_common_source_sessions_cannot_continue(tmp_path, first):
    _, parent, _, current, days, _, _, continuation = _published_parent(tmp_path)
    fewer = _replace_current_rows(current, first=days[first])
    with pytest.raises(q.BridgeError):
        q.audit_bridge(parent, fewer, continuation=continuation)


def test_suspended_common_dates_do_not_count_as_valid_parent_anchor_points(tmp_path):
    _, parent, _, current, _, _, _, continuation = _published_parent(tmp_path, suspended_days=(15, 16))
    # Both saved source vintages agree on these suspensions, but only 18 of
    # their 20 shared sessions have the required finite traded fields.
    with pytest.raises(q.BridgeError, match="20|overlap|common"):
        q.audit_bridge(parent, current, continuation=continuation)


@pytest.mark.parametrize("field", FIELDS)
def test_inherited_scale_does_not_bypass_any_real_field_mismatch(tmp_path, field):
    _, parent, _, current, _, _, _, continuation = _published_parent(tmp_path)
    path = parent / "features" / SYMBOL.lower() / f"{field}.day.bin"
    values = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
    values[21] *= 1.1
    path.write_bytes(values.tobytes())
    before = hashes(parent)
    target = tmp_path / "field-mismatch-must-not-publish"
    with pytest.raises(q.BridgeError):
        q.publish_increment(parent, current, target, continuation=continuation)
    assert not target.exists()
    assert hashes(parent) == before


def test_continuation_publication_keeps_every_old_feature_prefix_and_parent_byte(tmp_path):
    base, parent, _, current, days, _, _, continuation = _published_parent(tmp_path)
    base_before, parent_before = hashes(base), hashes(parent)
    original_features = {
        field: (parent / "features" / SYMBOL.lower() / f"{field}.day.bin").read_bytes()
        for field in FIELDS
    }
    child = tmp_path / "verified-child"
    manifest = q.publish_increment(parent, current, child, pilot_only=False, continuation=continuation)
    assert hashes(base) == base_before
    assert hashes(parent) == parent_before
    for field, old_bytes in original_features.items():
        new_bytes = (child / "features" / SYMBOL.lower() / f"{field}.day.bin").read_bytes()
        assert new_bytes[:len(old_bytes)] == old_bytes
        assert len(new_bytes) == len(old_bytes) + 5 * np.dtype("<f4").itemsize
    assert (child / "instruments/csi300.txt").read_bytes() == (parent / "instruments/csi300.txt").read_bytes()
    frame = q.read_qlib_security(child, SYMBOL)
    assert frame.loc[days[39]].notna().all()
    assert frame.loc[days[40]:].isna().all().all()
    assert manifest["securities"][SYMBOL]["after"]["last_complete_finite"] == days[39]
    assert "incremental_manifest.json" not in manifest["dataset_files"]
    for filename, digest in manifest["dataset_files"].items():
        assert hashlib.sha256((child / filename).read_bytes()).hexdigest() == digest


def test_last_security_rejection_never_publishes_the_successful_first_security(tmp_path, monkeypatch):
    _, parent, _, current, days, _, _, continuation = _published_parent(tmp_path, second_security=True)
    assert SYMBOL < OTHER_SYMBOL  # The intentionally bad security is last.
    path = parent / "features" / OTHER_SYMBOL.lower() / "volume.day.bin"
    values = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
    values[21] *= 1.1
    path.write_bytes(values.tobytes())
    checked = []
    original_check = q._error_stats

    def record_real_field_check(observed, expected, tolerance, label):
        checked.append(label)
        return original_check(observed, expected, tolerance, label)

    monkeypatch.setattr(q, "_error_stats", record_real_field_check)
    before = hashes(parent)
    target = tmp_path / "no-partial-two-stock-version"
    with pytest.raises(q.BridgeError):
        q.publish_increment(parent, current, target, pilot_only=False, continuation=continuation)
    assert {f"{SYMBOL}/{field}" for field in FIELDS}.issubset(checked)
    assert checked[-1] == f"{OTHER_SYMBOL}/volume"
    assert not target.exists()
    assert hashes(parent) == before
    assert not list(tmp_path.glob(".qlib-increment-stage-*"))
