"""The A5 market gate must verify fresh server echoes after a dirty socket."""
import hashlib
import json
from pathlib import Path

import pytest

from stopat30m.data.a5_market_audit import audit_dividend_source_correction


def save(path, value):
    path.write_text(json.dumps(value))
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def fixture(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    request = {"code": "sh.600000", "year": "2024", "yearType": "operate"}
    response = source / "SH600000_2024.json"
    new = save(response, {"request": request, "fields": ["code", "dividOperateDate"], "rows": [],
        "fetched_at": "2026-10-20T01:02:03+00:00"})
    old_record = {"file": "SH600000_2023.json", "sha256": "trusted-prefix-bytes"}
    old_manifest = save(tmp_path / "old_manifest.json", {"instruments": ["SH600000"], "years": [2023, 2024],
        "responses": {"SH600000_2023": old_record}})
    progress = save(tmp_path / "progress.json", {"failed_transport_observed": True})
    quarantine = save(tmp_path / "quarantine.json", {"kind": "a5_dividend_source_quarantine_receipt",
        "status": "quarantined_pending_strict_recapture", "generation": "same-generation", "source_as_of_date": "2026-10-20",
        "original_manifest": old_manifest, "original_progress": progress,
        "selected_tail_keys": ["SH600000_2024"],
        "first_untrusted_query": {"ordinal_1based": 2, "key": "SH600000_2024"}, "quarantined_responses": [],
        "finished_at": "2026-10-20T01:00:00+00:00"})
    manifest = save(source / "collection.json", {"source_correction_required": True,
        "generation": "same-generation", "as_of_date": "2026-10-20", "responses": {
            "SH600000_2023": old_record, "SH600000_2024": {"file": response.name, "sha256": new["sha256"]}}})
    guard = save(tmp_path / "guard.json", {"request": request, "server_echo": request,
        "server_error_code": "0", "status": "verified", "rows_verified": 0,
        "complete_row_iteration_observed": True, "started_at": "2026-10-20T01:01:00+00:00",
        "finished_at": "2026-10-20T01:02:00+00:00"})
    correction_path = tmp_path / "correction.json"
    correction = {"kind": "a5_dividend_source_correction_receipt", "status": "passed",
        "generation": "same-generation", "source_as_of_date": "2026-10-20",
        "source_collection_manifest": manifest, "quarantine_receipt": quarantine,
        "corrected_response_keys": ["SH600000_2024"], "corrected_responses": [{"key": "SH600000_2024",
            "new_response": new, "new_response_fetched_at": "2026-10-20T01:02:03+00:00",
            "server_echo_guard": guard, "server_echo": request, "rows_verified": 0, "old_quarantined_response": None}]}
    save(correction_path, correction)
    return source, correction_path, correction


def test_known_dirty_socket_source_requires_completed_receipt(tmp_path):
    source, _, _ = fixture(tmp_path)
    with pytest.raises(ValueError, match="unique completed correction"):
        audit_dividend_source_correction(source)


def test_empty_tail_response_is_usable_only_with_its_verified_fresh_server_echo(tmp_path):
    source, receipt, _ = fixture(tmp_path)
    proofs = audit_dividend_source_correction(source, [receipt])
    assert "server_echo_guard:SH600000_2024" in proofs
    assert "quarantined_original_manifest" in proofs


def test_stale_year_echo_cannot_be_hidden_by_an_empty_response(tmp_path):
    source, receipt, correction = fixture(tmp_path)
    path = Path(correction["corrected_responses"][0]["server_echo_guard"]["path"])
    guard = json.loads(path.read_text())
    guard["server_echo"] = {**guard["request"], "year": "2023"}
    correction["corrected_responses"][0]["server_echo_guard"] = save(path, guard)
    save(receipt, correction)
    with pytest.raises(ValueError, match="exact successful server echo"):
        audit_dividend_source_correction(source, [receipt])


def test_guard_observed_before_quarantine_cannot_certify_recapture(tmp_path):
    source, receipt, correction = fixture(tmp_path)
    path = Path(correction["corrected_responses"][0]["server_echo_guard"]["path"])
    guard = json.loads(path.read_text())
    guard["started_at"] = "2026-10-20T00:59:59+00:00"
    correction["corrected_responses"][0]["server_echo_guard"] = save(path, guard)
    save(receipt, correction)
    with pytest.raises(ValueError, match="after quarantine"):
        audit_dividend_source_correction(source, [receipt])


def test_partial_dirty_tail_cannot_be_published_as_complete(tmp_path):
    source, receipt, correction = fixture(tmp_path)
    correction["corrected_response_keys"] = []
    save(receipt, correction)
    with pytest.raises(ValueError, match="complete untrusted connection tail"):
        audit_dividend_source_correction(source, [receipt])
