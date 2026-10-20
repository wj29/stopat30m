"""The public collection CLI cannot turn arbitrary tickers into allowed targets."""
import hashlib
import json
from pathlib import Path

from click.testing import CliRunner
import pandas as pd
import pytest

from stopat30m.data.ashare_collection_scope import SCOPE_RECEIPT
from stopat30m.data.ashare_scope import derive_code_validity_intervals
from stopat30m.research_cli import research_group


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def derived_scope(tmp_path):
    """Actual derivation/loader, with explicitly synthetic full-size membership."""
    original = tmp_path / "original"
    original.mkdir()
    paths = {}
    for name, prefix, count in (("csi300", 600000, 300), ("csi1000", 300000, 1000)):
        members = [("SH" if name == "csi300" else "SZ") + str(prefix + n) for n in range(count)]
        if name == "csi1000":
            members.append("SZ302132")  # Same issuer's not-yet-valid new code.
        path = original / (name + ".txt")
        path.write_text("".join(f"{symbol}\t2023-01-01\t2024-12-31\n" for symbol in members))
        paths[name] = path
    sources = []
    for name in ("implementation", "reminder"):
        path = original / (name + ".pdf")
        path.write_bytes(("Synthetic issuer code-validity evidence: " + name).encode())
        sources.append({"path": str(path), "sha256": digest(path), "url": "https://example.test/" + name})
    evidence = original / "code-validity.json"
    evidence.write_text(json.dumps({"schema_version": 1, "kind": "ashare_reviewed_code_validity_evidence",
        "code_changes": [{"old_instrument": "SZ300114", "new_instrument": "SZ302132",
            "effective_date": "2025-02-17", "same_legal_issuer": True,
            "share_quantity_changes": False, "source_evidence": sources}]}))
    calendar = original / "calendar.txt"
    calendar.write_text("2023-01-03\n2023-01-04\n2023-01-05\n2023-01-06\n")
    output = tmp_path / "derived"
    report = derive_code_validity_intervals(paths, evidence, calendar, output,
        start="2023-01-03", end="2023-01-06")
    assert report["status"] == "passed"
    return output, original


@pytest.fixture
def collector(monkeypatch):
    from stopat30m.data import ashare_daily
    calls = []
    def collect(output, start, end, **kwargs):
        calls.append((output, start, end, kwargs))
        output.mkdir(parents=True, exist_ok=True)
        manifest = {"request_start": start, "request_end": end,
            "instruments": kwargs["instruments"] or ["SH600000"],
            "target_selection": {"kind": "explicit_download_targets" if kwargs["instruments"] is not None else "baostock_index_snapshot_for_download_targets_only"}}
        (output / "collection.json").write_text(json.dumps(manifest))
        return output
    monkeypatch.setattr(ashare_daily, "collect_baostock_increment", collect)
    monkeypatch.setattr(ashare_daily, "load_collection_frames", lambda output:
        (pd.DataFrame([{"instrument": "SH600000"}]), None, json.loads((output / "collection.json").read_text())))
    return calls


def invoke(output, *, instruments="SH600000", scope=None, start="2023-01-03", end="2023-01-06", extra=()):
    args = ["ashare-collect", "--start-date", start, "--end-date", end, "--output-dir", str(output)]
    if instruments is not None:
        args.extend(["--instruments", instruments])
    if scope is not None:
        args.extend(["--historical-scope-dir", str(scope)])
    return CliRunner().invoke(research_group, [*args, *extra])


@pytest.mark.parametrize("instruments", ["SH600000", "SH601999", ""])
def test_explicit_targets_without_bound_scope_never_call_collector(tmp_path, collector, instruments):
    output = tmp_path / "never-created"
    result = invoke(output, instruments=instruments)
    assert result.exit_code == 1 and "requires --historical-scope-dir" in result.output
    assert collector == [] and not output.exists()


@pytest.mark.parametrize("instruments", ["SH601999", "SH600000,SH601999", "SZ302132", "SH600000,"])
def test_outsider_future_code_or_empty_token_is_rejected_before_any_collection(tmp_path, derived_scope, collector, instruments):
    scope, _ = derived_scope
    output = tmp_path / "never-created"
    result = invoke(output, instruments=instruments, scope=scope)
    assert result.exit_code == 1, result.output
    assert "outside" in result.output or "must not be empty" in result.output
    assert collector == [] and not output.exists()


def test_legal_subset_uses_real_scope_loaders_and_preserves_verified_input_binding(tmp_path, derived_scope, collector):
    scope, original = derived_scope
    output = tmp_path / "new-collection"
    result = invoke(output, instruments="sz300001,SH600001,SH600001", scope=scope)
    assert result.exit_code == 0, result.output
    assert len(collector) == 1 and collector[0][3]["instruments"] == ["SH600001", "SZ300001"]
    receipt_path = output / SCOPE_RECEIPT
    original_receipt = receipt_path.read_bytes()
    receipt = json.loads(original_receipt)
    assert receipt["request_start"] == "2023-01-03" and receipt["request_end"] == "2023-01-06"
    assert receipt["instruments"] == ["SH600001", "SZ300001"]
    assert len(receipt["eligible_download_targets"]) == 1300
    assert receipt["requested_exchange_sessions"] == (original / "calendar.txt").read_text().splitlines()
    assert receipt["bound_inputs"]["original_calendar"] == {"path": str(original / "calendar.txt"), "sha256": digest(original / "calendar.txt")}
    assert receipt["bound_inputs"]["historical:derivation:manifest"]["sha256"] == digest(scope / "manifest.json")
    for proof in receipt["bound_inputs"].values():
        assert Path(proof["path"]).is_absolute() and digest(Path(proof["path"])) == proof["sha256"]
    assert receipt["announcement_time_PIT"] is False
    again = invoke(output, instruments="SH600001,SZ300001", scope=scope)
    assert again.exit_code == 0, again.output
    assert len(collector) == 2 and receipt_path.read_bytes() == original_receipt


@pytest.mark.parametrize("target", ["calendar.txt", "csi300.txt", "implementation.pdf"])
def test_changed_original_evidence_fails_before_any_collector_call(tmp_path, derived_scope, collector, target):
    scope, original = derived_scope
    (original / target).write_bytes((original / target).read_bytes() + b"changed")
    output = tmp_path / "never-created"
    result = invoke(output, scope=scope)
    assert result.exit_code == 1 and "checksum differs" in result.output
    assert collector == [] and not output.exists()


@pytest.mark.parametrize("start,end", [("2023-01-02", "2023-01-06"), ("2023-01-03", "2023-01-09"),
    ("2023-01-07", "2023-01-08"), ("2023-01-06", "2023-01-03"), ("20230103", "2023-01-06")])
def test_uncovered_or_invalid_dates_fail_before_network(tmp_path, derived_scope, collector, start, end):
    scope, _ = derived_scope
    output = tmp_path / "never-created"
    result = invoke(output, scope=scope, start=start, end=end)
    assert result.exit_code == 1, result.output
    assert collector == [] and not output.exists()


def test_conflicting_resume_cannot_replace_existing_binding_or_collection(tmp_path, derived_scope, collector):
    scope, _ = derived_scope
    output = tmp_path / "collection"
    assert invoke(output, scope=scope).exit_code == 0
    saved = {name: (output / name).read_bytes() for name in (SCOPE_RECEIPT, "collection.json")}
    result = invoke(output, scope=scope, instruments="SZ300000")
    assert result.exit_code == 1 and "use a new collection directory" in result.output
    assert len(collector) == 1
    assert all((output / name).read_bytes() == data for name, data in saved.items())
    no_scope = invoke(output, instruments=None)
    assert no_scope.exit_code == 1 and "bound to explicit" in no_scope.output
    assert len(collector) == 1


def test_a_passed_derivation_cannot_extend_the_original_calendar_tail(tmp_path, derived_scope, collector):
    _, original = derived_scope
    scope = tmp_path / "overstated-audit-window"
    report = derive_code_validity_intervals(
        {name: original / (name + ".txt") for name in ("csi300", "csi1000")},
        original / "code-validity.json", original / "calendar.txt", scope,
        start="2023-01-03", end="2023-01-09")
    assert report["status"] == "passed"  # Its four observed sessions pass.
    output = tmp_path / "never-created"
    result = invoke(output, scope=scope, end="2023-01-09")
    assert result.exit_code == 1 and "original calendar coverage" in result.output
    assert collector == [] and not output.exists()


def test_an_old_unbound_collection_cannot_gain_retrospective_scope_provenance(tmp_path, derived_scope, collector):
    scope, _ = derived_scope
    output = tmp_path / "old-collection"
    output.mkdir()
    (output / "collection.json").write_bytes(b"preserve original unbound generation")
    result = invoke(output, scope=scope)
    assert result.exit_code == 1 and "has no historical scope binding" in result.output
    assert collector == [] and not (output / SCOPE_RECEIPT).exists()
    assert (output / "collection.json").read_bytes() == b"preserve original unbound generation"


def test_default_hs300_download_remains_an_allowed_subset_without_historical_claim(tmp_path, collector):
    output = tmp_path / "default-index"
    result = invoke(output, instruments=None, extra=("--index-date", "2023-01-06"))
    assert result.exit_code == 0, result.output
    assert len(collector) == 1 and collector[0][3]["instruments"] is None
    assert collector[0][3]["index_date"] == "2023-01-06"
    assert not (output / SCOPE_RECEIPT).exists()


def test_scoped_receipt_detects_source_mutation_during_collection(tmp_path, derived_scope, collector, monkeypatch):
    from stopat30m.data import ashare_daily
    scope, original = derived_scope
    collect = ashare_daily.collect_baostock_increment
    def changed(*args, **kwargs):
        output = collect(*args, **kwargs)
        source = original / "calendar.txt"
        source.write_text(source.read_text() + "2023-01-09\n")
        return output
    monkeypatch.setattr(ashare_daily, "collect_baostock_increment", changed)
    output = tmp_path / "quarantined"
    result = invoke(output, scope=scope)
    assert result.exit_code == 1 and "changed after binding" in result.output
    assert len(collector) == 1 and (output / SCOPE_RECEIPT).exists()
