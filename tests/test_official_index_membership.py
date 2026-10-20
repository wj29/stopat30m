"""Official effective sessions, inverse-event integrity and source-bound scope dispatch."""
import copy
import hashlib
import json

import pytest

from stopat30m.data.ashare_scope import AshareScopeError, load_verified_historical_scope
from stopat30m.data.official_index_membership import (
    SOURCE_CHECKS, build_official_reconstruction, event_first_session,
    reconstruct_membership, verify_official_reconstruction,
)


def _fixture(tmp_path):
    days = ["2024-01-02", "2024-01-03", "2024-01-08", "2024-01-09", "2024-01-10"]
    # Explicit synthetic sessions include a holiday gap; they are not a real feed.
    anchor = {"as_of_date": days[-1], "members": {
        "csi300": [f"SH{600000+x}" for x in range(300)],
        "csi1000": [f"SZ{x:06d}" for x in range(1000)]}}
    original = tmp_path / "official_source.json"
    original.write_text('{"synthetic_original":true}\n')
    proof = {"path": str(original.resolve()), "sha256": hashlib.sha256(original.read_bytes()).hexdigest()}
    event = {"id": "csi1000:synthetic", "scope": "csi1000", "published_on": days[0],
        "known_at": days[0] + "T23:59:59+08:00", "effective_on": days[1], "effective_basis": "after_close",
        "first_effective_session": days[2], "adds": ["SZ000999"], "removes": ["SZ001001"], "evidence": [proof]}
    return days, anchor, event, proof


def test_close_effective_change_uses_next_actual_session_not_next_weekday(tmp_path):
    days, _, event, _ = _fixture(tmp_path)
    assert event_first_session(event, days) == "2024-01-08"
    bad = {**event, "first_effective_session": "2024-01-04"}
    with pytest.raises(ValueError, match="semantics"):
        event_first_session(bad, days)


def test_inverse_replay_preserves_old_members_until_official_effective_session(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    result = reconstruct_membership(days, anchor, [event], start=days[0], end=days[-1])
    assert "SZ001001\t2024-01-02\t2024-01-03\n" in result["intervals"]["csi1000"]
    assert "SZ000999\t2024-01-08\t2024-01-10\n" in result["intervals"]["csi1000"]
    assert len(result["sessions"]) == 5
    assert result["reversed_event_ids"] == [event["id"]]


def test_wrong_later_side_cannot_be_repaired_by_count_matching(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    event["adds"] = ["SZ009999"]
    with pytest.raises(ValueError, match="independently observed"):
        reconstruct_membership(days, anchor, [event], start=days[0], end=days[-1])


def test_same_day_cross_index_migrations_are_reversed_as_one_batch(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    e300 = {**event, "id": "csi300:migration", "scope": "csi300", "adds": ["SH600000"], "removes": ["SZ000999"]}
    e1000 = {**event, "id": "csi1000:migration", "adds": ["SZ000999"], "removes": ["SH600000"]}
    result = reconstruct_membership(days, anchor, [e300, e1000], start=days[0], end=days[-1])
    assert "SZ000999\t2024-01-02\t2024-01-03\n" in result["intervals"]["csi300"]
    assert all(row["overlap"] == 0 for row in result["sessions"])


def _build(tmp_path):
    days, anchor, event, proof = _fixture(tmp_path)
    calendar = tmp_path / "day.txt"
    calendar.write_text("\n".join(days) + "\n")
    events = tmp_path / "official_events.json"
    events.write_text(json.dumps({"schema_version": 1, "kind": "ashare_official_index_adjustment_events",
        "anchor": anchor, "events": [event], "bound_inputs": {"original": proof}}))
    review = tmp_path / "coverage_review.json"
    review.write_text(json.dumps({"kind": "a5_official_index_event_coverage_review", "status": "passed",
        "start": days[0], "end": days[-1], "checks": {check: True for check in SOURCE_CHECKS},
        "bound_inputs": {"original": proof}}))
    output = tmp_path / "official_scope"
    build_official_reconstruction(calendar, events, review, output, start=days[0], end=days[-1])
    return output, proof, days


def test_official_scope_loader_verifies_semantic_replay_and_every_original(tmp_path):
    output, proof, days = _build(tmp_path)
    scope = load_verified_historical_scope(output, start=days[0], end=days[-1], require_official=True)
    assert "SZ001001" in scope.members_on(days[0]) and "SZ000999" not in scope.members_on(days[0])
    assert "SZ000999" in scope.members_on(days[-1])
    from pathlib import Path
    Path(proof["path"]).write_text("changed\n")
    with pytest.raises(AshareScopeError, match="changed"):
        scope.assert_unchanged()
    with pytest.raises(ValueError, match="checksum"):
        verify_official_reconstruction(output, start=days[0], end=days[-1])


def test_counts_alone_do_not_authorize_an_unreviewed_catalogue(tmp_path):
    days, anchor, event, proof = _fixture(tmp_path)
    calendar = tmp_path / "day.txt"
    calendar.write_text("\n".join(days) + "\n")
    events = tmp_path / "events.json"
    events.write_text(json.dumps({"schema_version": 1, "kind": "ashare_official_index_adjustment_events",
        "anchor": anchor, "events": [event], "bound_inputs": {"original": proof}}))
    review = tmp_path / "review.json"
    review.write_text(json.dumps({"kind": "a5_official_index_event_coverage_review", "status": "blocked",
        "start": days[0], "end": days[-1], "checks": {check: False for check in SOURCE_CHECKS},
        "bound_inputs": {"original": proof}}))
    with pytest.raises(ValueError, match="counts alone"):
        build_official_reconstruction(calendar, events, review, tmp_path / "output", start=days[0], end=days[-1])


def test_v1_code_clipping_cannot_impersonate_official_reconstruction(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps({"kind": "ashare_code_validity_interval_derivation"}))
    with pytest.raises(AshareScopeError, match="official membership"):
        load_verified_historical_scope(tmp_path, start="2023-01-03", end="2024-12-31", require_official=True)


def _code_event(event, *, old="SZ001001", new="SZ000999"):
    return {key: value for key, value in {**event, "id": "issuer_code_change", "event_type": "same_issuer_security_code_change",
        "old_instrument": old, "new_instrument": new, "same_legal_issuer": True, "share_quantity_changes": False,
        "price_mapping_allowed": False}.items() if key not in {"scope", "adds", "removes"}}


def test_verified_code_change_restores_only_a_member_found_in_independent_later_state(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    result = reconstruct_membership(days, anchor, [_code_event(event)], start=days[0], end=days[-1])
    assert "SZ001001\t2024-01-02\t2024-01-03\n" in result["intervals"]["csi1000"]
    assert result["code_change_applications"][0]["scopes_restored_from_independently_verified_later_membership"] == ["csi1000"]
    result = reconstruct_membership(days, anchor, [_code_event(event, old="SZ009998", new="SZ009999")],
        start=days[0], end=days[-1])
    assert result["code_change_applications"][0]["scopes_restored_from_independently_verified_later_membership"] == []
    assert "SZ009998" not in result["intervals"]["csi1000"]


def test_same_day_new_index_entry_does_not_backfill_its_old_issuer_code(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    event["removes"] = ["SZ001002"]
    result = reconstruct_membership(days, anchor, [event, _code_event(event)], start=days[0], end=days[-1])
    assert "SZ001002\t2024-01-02\t2024-01-03\n" in result["intervals"]["csi1000"]
    assert "SZ001001" not in result["intervals"]["csi1000"]
    assert result["code_change_applications"][0]["scopes_restored_from_independently_verified_later_membership"] == []


def test_same_day_index_exit_can_explicitly_restore_an_old_code_without_price_mapping(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    code_event = _code_event(event, old="SZ001001", new="SZ001002")
    result = reconstruct_membership(days, anchor, [event, code_event], start=days[0], end=days[-1])
    assert "SZ001001\t2024-01-02\t2024-01-03\n" in result["intervals"]["csi1000"]
    assert "SZ001002" not in result["intervals"]["csi1000"]


def test_code_change_cannot_synthesize_shares_or_hide_simultaneous_old_new_membership(tmp_path):
    days, anchor, event, _ = _fixture(tmp_path)
    bad = _code_event(event, old="SZ000998")
    with pytest.raises(ValueError, match="Both old and new"):
        reconstruct_membership(days, anchor, [bad], start=days[0], end=days[-1])
    bad = _code_event(event)
    bad["share_quantity_changes"] = True
    with pytest.raises(ValueError, match="allowed index"):
        reconstruct_membership(days, anchor, [bad], start=days[0], end=days[-1])


def test_verified_batch_hashes_each_unique_source_at_entry_and_exit(tmp_path, monkeypatch):
    from collections import Counter
    from stopat30m.data.ashare_scope import SourceProof
    output, _, days = _build(tmp_path)
    scope = load_verified_historical_scope(output, start=days[0], end=days[-1], require_official=True)
    original = SourceProof.assert_unchanged
    calls = Counter()
    def count(self):
        calls[(self.path, self.sha256)] += 1
        return original(self)
    monkeypatch.setattr(SourceProof, "assert_unchanged", count)
    with scope.verified_batch():
        for day in days:
            assert len(scope.members_on(day)) == 1300
        assert set(calls.values()) == {1}
    assert set(calls.values()) == {2}
    scope.members_on(days[0])
    assert set(calls.values()) == {3}


def test_batch_exit_detects_same_size_same_mtime_source_tampering_even_on_body_error(tmp_path):
    from pathlib import Path
    import os
    output, source, days = _build(tmp_path)
    scope = load_verified_historical_scope(output, start=days[0], end=days[-1], require_official=True)
    path = Path(source["path"])
    original = path.read_bytes()
    stat = path.stat()
    with pytest.raises(AshareScopeError, match="changed"):
        with scope.verified_batch():
            path.write_bytes(original.replace(b"true", b"null"))
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            assert len(scope.members_on(days[0])) == 1300  # Read-only parsed state.
            raise RuntimeError("Caller fails before publishing output")
    path.write_bytes(original)
    with scope.verified_batch():
        assert len(scope.members_on(days[0])) == 1300


def test_verified_batch_is_thread_owned_non_reentrant_and_recovers_after_exception(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    output, _, days = _build(tmp_path)
    scope = load_verified_historical_scope(output, start=days[0], end=days[-1], require_official=True)
    with scope.verified_batch():
        with ThreadPoolExecutor(max_workers=1) as worker:
            other = worker.submit(scope.members_on, days[0])
            with pytest.raises(AshareScopeError, match="across threads"):
                other.result()
        with pytest.raises(AshareScopeError, match="already active"):
            with scope.verified_batch():
                pass
    with pytest.raises(RuntimeError, match="ordinary"):
        with scope.verified_batch():
            raise RuntimeError("ordinary caller failure")
    assert len(scope.members_on(days[0])) == 1300
