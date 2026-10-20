import hashlib

import numpy as np
import pytest

from stopat30m.data.a5_price_window import clip_field_bytes, _window_policy, SPEC_PATH


def test_uniform_clipping_preserves_nan_payload_bits_and_only_rebases_offset():
    # A noncanonical NaN payload must remain bit-exact as well as a NaN value.
    raw = np.asarray([2.], dtype="<f4").tobytes() + np.asarray(
        [0x3f800000, 0x7fc01234, 0x40000000, 0x40400000], dtype="<u4").tobytes()
    clipped, receipt = clip_field_bytes(raw, old_calendar_length=9, first=3, last=5)
    assert np.frombuffer(clipped, dtype="<f4")[0] == 0
    assert clipped[4:] == raw[8:]
    assert receipt["retained_payload_sha256"] == hashlib.sha256(raw[8:]).hexdigest()
    assert receipt["retained_observation_count"] == 3


def test_ipo_after_window_start_keeps_actual_later_calendar_offset():
    raw = np.asarray([5, 10, 11, 12], dtype="<f4").tobytes()
    clipped, receipt = clip_field_bytes(raw, old_calendar_length=9, first=2, last=6)
    assert np.frombuffer(clipped, dtype="<f4").tolist() == [3, 10, 11]
    assert receipt["retained_original_byte_range"] == [4, 12]


@pytest.mark.parametrize("offset,values,length", [(-1, [1], 3), (.5, [1], 3), (2, [1, 2], 3), (0, [float("inf")], 3)])
def test_window_view_rejects_malformed_parent_instead_of_clipping_damage(offset, values, length):
    raw = np.asarray([offset, *values], dtype="<f4").tobytes()
    with pytest.raises(ValueError, match="Malformed"):
        clip_field_bytes(raw, old_calendar_length=length, first=0, last=2)


def test_window_without_source_observations_does_not_create_synthetic_nan_history():
    raw = np.asarray([0, 1, 2], dtype="<f4").tobytes()
    with pytest.raises(ValueError, match="no observations"):
        clip_field_bytes(raw, old_calendar_length=6, first=3, last=5)


def test_policy_is_uniform_and_excludes_performance_driven_or_inside_window_row_drops():
    policy = _window_policy(SPEC_PATH)
    assert policy["first_membership_and_training_year"] == 2018
    assert policy["requested_price_start"] == "2017-01-01"
    assert not policy["performance_used_to_choose_window"]
    assert not policy["drop_bad_rows_inside_declared_window"]
