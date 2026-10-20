"""Apply the project membership gate before an explicit BaoStock collection.

The existing collector stays unchanged. Its original responses are accompanied
by a deterministic receipt binding the exact request to verified scope inputs.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path

from .ashare_scope import (
    DEFAULT_SCOPE_DECLARATION, AshareScopeError, HistoricalAshareScope,
    ProjectAshareScope, SourceProof,
)


SCOPE_RECEIPT = "historical_scope_binding.json"


def _day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise AshareScopeError("Collection scope dates must use YYYY-MM-DD")
    return value


def _explicit_binding(directory, start, end, instruments, index_date):
    start, end = _day(start), _day(end)
    if start > end:
        raise AshareScopeError("Collection scope dates are reversed")
    if not instruments or any(not isinstance(symbol, str) or not symbol for symbol in instruments):
        raise AshareScopeError("Explicit download targets must not be empty")
    root = Path(directory).expanduser().resolve()
    historical = HistoricalAshareScope.from_derived(root, start=start, end=end)
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("status") != "passed" or "audit.json" not in manifest["files"]:
        raise AshareScopeError("Historical scope derivation must have a passed bound audit")
    audit = json.loads((root / "audit.json").read_text())
    if (audit.get("kind") != "ashare_code_validity_interval_audit"
            or audit.get("status") != "passed"
            or not _day(audit["audit_start"]) <= start <= end <= _day(audit["audit_end"])):
        raise AshareScopeError("Requested dates exceed the verified historical scope audit coverage")
    calendar_proof = manifest["bound_inputs"]["original_calendar"]
    calendar = SourceProof.read(calendar_proof["path"], calendar_proof["sha256"])
    sessions = calendar.path.read_text().splitlines()
    if not sessions or sessions != sorted(set(sessions)):
        raise AshareScopeError("Require the bound original ordered exchange calendar")
    sessions = [_day(day) for day in sessions]
    if not sessions[0] <= start <= end <= sessions[-1]:
        raise AshareScopeError("Requested dates exceed the bound original calendar coverage")
    audited_days = [row["as_of_date"] for row in audit["derived_audit"]["sessions"]]
    if (audit["derived_audit"].get("status") != "passed"
            or audited_days != [day for day in sessions if audit["audit_start"] <= day <= audit["audit_end"]]):
        raise AshareScopeError("Historical scope audit does not cover its original calendar sessions")
    declaration = SourceProof.read(DEFAULT_SCOPE_DECLARATION)
    project = ProjectAshareScope.from_file(declaration.path, historical=historical)
    allowed = project.download_targets(start, end, sessions=sessions)
    selected = sorted(set(instruments))
    if not set(selected).issubset(allowed):
        raise AshareScopeError("Explicit download targets are outside the verified CSI300/CSI1000 historical scope")
    proofs = {"historical:" + name: SourceProof(Path(item["path"]), item["sha256"])
        for name, item in historical.source_receipts().items()}
    proofs.update({"derived:" + name: SourceProof.read(root / name, digest)
        for name, digest in manifest["files"].items()})
    proofs.update(project_scope=declaration, original_calendar=calendar,
        collection_scope_implementation=SourceProof.read(__file__))
    project.assert_unchanged()
    for proof in proofs.values():
        proof.assert_unchanged()
    receipt = {"schema_version": 1, "kind": "ashare_historical_collection_scope_binding",
        "scope": "union", "allowed_indices": ["csi300", "csi1000"],
        "request_start": start, "request_end": end, "instruments": selected, "index_date": index_date,
        "historical_scope_dir": str(root), "audit_start": audit["audit_start"], "audit_end": audit["audit_end"],
        "requested_exchange_sessions": [day for day in sessions if start <= day <= end],
        "eligible_download_targets": allowed,
        "bound_inputs": {name: proof.to_dict() for name, proof in sorted(proofs.items())},
        "announcement_time_PIT": False, "daily_membership_still_required_for_training_and_trading": True}
    return receipt, proofs


def collect_scope_checked_increment(output_dir, start_date, end_date, *, instruments=None,
        historical_scope_dir=None, index_date=None, progress=None):
    """Reject unaudited explicit requests before calling any market collector."""
    from .ashare_daily import collect_baostock_increment
    from .qlib_snapshot import guard_qlib_output_path

    output = guard_qlib_output_path(output_dir)
    receipt_path = output / SCOPE_RECEIPT
    if instruments is None:
        if historical_scope_dir is not None:
            raise AshareScopeError("--historical-scope-dir requires explicit --instruments")
        if receipt_path.exists():
            raise AshareScopeError("This collection is bound to explicit historical targets; use its same request or a new directory")
        return collect_baostock_increment(output, start_date, end_date,
            index_date=index_date, instruments=None, progress=progress)
    if historical_scope_dir is None:
        raise AshareScopeError("Explicit --instruments requires --historical-scope-dir with verified historical scope")
    receipt, proofs = _explicit_binding(historical_scope_dir, start_date, end_date, instruments, index_date)
    # An existing unbound generation cannot acquire retrospective provenance.
    if not receipt_path.exists() and output.exists() and any(output.iterdir()):
        raise AshareScopeError("Existing collection has no historical scope binding; preserve it and use a new directory")
    output.mkdir(parents=True, exist_ok=True)
    import fcntl
    with (output / ".historical_scope.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if receipt_path.exists():
            if json.loads(receipt_path.read_text()) != receipt:
                raise AshareScopeError("Historical scope request or bound inputs differ; use a new collection directory")
        else:
            with receipt_path.open("x", encoding="utf-8") as stream:
                stream.write(json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        for proof in proofs.values():
            proof.assert_unchanged()
        result = collect_baostock_increment(output, start_date, end_date,
            index_date=index_date, instruments=receipt["instruments"], progress=progress)
        for proof in proofs.values():
            proof.assert_unchanged()
        if json.loads(receipt_path.read_text()) != receipt:
            raise AshareScopeError("Historical collection scope binding changed during collection")
        return result
