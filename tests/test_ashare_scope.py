"""Only dated CSI300/CSI1000 views may admit buys; existing exposure stays manageable."""
import hashlib
import json
from pathlib import Path

import pytest

from stopat30m.data.ashare_scope import (
    AshareScopeError, CurrentAshareScope, HistoricalAshareScope, access_allowed,
)


DAY = "2026-10-19"
FETCHED = "2026-10-20T09:59:27+08:00"


def symbols():
    # Explicitly synthetic constituent identities, not a real-market fixture.
    return {"csi300": [f"SH{600000+i:06d}" for i in range(300)],
        "csi1000": [f"SZ{300000+i:06d}" for i in range(1000)]}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def historical_files(tmp_path, *, migrate=False, later_smallcap_start=False):
    members = symbols()
    paths = {}
    for name, instruments in members.items():
        lines = []
        for instrument in instruments:
            if migrate and instrument == instruments[0]:
                lines.append(f"{instrument}\t2023-01-01\t2023-12-31")
                other = members["csi1000" if name == "csi300" else "csi300"][0]
                lines.append(f"{other}\t2024-01-01\t2024-12-31")
            else:
                start = "2024-01-01" if later_smallcap_start and name == "csi1000" else "2023-01-01"
                lines.append(f"{instrument}\t{start}\t2024-12-31")
        paths[name] = tmp_path / (name + ".txt")
        paths[name].write_text("\n".join(lines) + "\n")
    return paths


def current_files(tmp_path):
    paths = {}
    for name, members in symbols().items():
        source = tmp_path / (name + "-official-source.xls")
        source.write_bytes(("synthetic original source for " + name).encode())
        value = {"kind": "ashare_current_scope_observation", "scope": name, "as_of_date": DAY,
            "fetched_at": FETCHED, "members": members, "source_evidence": [{"path": source.name,
                "sha256": digest(source), "url": "https://example.test/index/" + name, "fetched_at": FETCHED}]}
        paths[name] = tmp_path / (name + ".json")
        paths[name].write_text(json.dumps(value))
    return paths


def revise(path, callback):
    value = json.loads(path.read_text())
    callback(value)
    path.write_text(json.dumps(value))


@pytest.mark.parametrize("factory", [historical_files, current_files])
@pytest.mark.parametrize("unsupported", ["csi500", "all", "hs300", "CSI300"])
def test_other_scopes_cannot_enter_through_source_mapping(tmp_path, factory, unsupported):
    paths = factory(tmp_path)
    paths[unsupported] = paths["csi300"]
    loader = HistoricalAshareScope.from_files if factory is historical_files else CurrentAshareScope.from_files
    kwargs = {"start": "2023-01-01", "end": "2024-12-31"} if factory is historical_files else {"as_of_date": DAY}
    with pytest.raises(AshareScopeError, match="other scopes are unsupported"):
        loader(paths, **kwargs)


def test_historical_membership_migration_uses_each_effective_day_without_today_backfill(tmp_path):
    paths = historical_files(tmp_path, migrate=True)
    scope = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    large, small = symbols()["csi300"][0], symbols()["csi1000"][0]
    assert large in scope.members_on("2023-06-01", scope="csi300")
    assert large not in scope.members_on("2024-06-03", scope="csi300")
    assert small not in scope.members_on("2023-06-01", scope="csi300")
    assert small in scope.members_on("2024-06-03", scope="csi300")
    for day in ("2023-06-01", "2024-06-03"):
        view = scope.view_on(day)
        assert view["member_count"] == 1300 and view["overlap"] == []
        assert view["index_counts"] == {"csi300": 300, "csi1000": 1000}


def test_historical_missing_smallcap_history_cannot_use_current_or_future_members(tmp_path):
    paths = historical_files(tmp_path, later_smallcap_start=True)
    scope = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    assert len(scope.members_on("2023-01-03", scope="csi300")) == 300
    with pytest.raises(AshareScopeError, match="csi1000:count_0_expected_1000"):
        scope.members_on("2023-01-03")
    audit = scope.audit_sessions(["2023-01-03", "2024-01-02"])
    assert audit["status"] == "blocked"
    assert audit["sessions"][0]["index_counts"]["csi1000"] == 0
    assert audit["sessions"][1]["usable"]


def test_original_historical_files_and_checksums_are_immutable(tmp_path):
    paths = historical_files(tmp_path)
    expected = {name: digest(path) for name, path in paths.items()}
    scope = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31", expected_sha256=expected)
    assert scope.source_receipts()["csi1000"]["sha256"] == expected["csi1000"]
    paths["csi1000"].write_text(paths["csi1000"].read_text().replace("2024-12-31", "2024-12-30", 1))
    with pytest.raises(AshareScopeError, match="changed after binding"):
        scope.members_on("2023-01-03")
    with pytest.raises(AshareScopeError, match="checksum differs"):
        HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31", expected_sha256=expected)


def test_historical_alias_outside_window_is_preserved_but_intersecting_alias_is_rejected(tmp_path):
    paths = historical_files(tmp_path)
    original = paths["csi1000"].read_text()
    paths["csi1000"].write_text(original + "SHT00018\t2005-01-01\t2006-01-01\n")
    scope = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    audit = scope.audit_sessions(["2023-01-03"])
    assert audit["outside_window_aliases_preserved_unmapped"]["csi1000"][0]["instrument"] == "SHT00018"
    assert paths["csi1000"].read_text().endswith("SHT00018\t2005-01-01\t2006-01-01\n")
    paths["csi1000"].write_text(original + "SHT00018\t2023-01-01\t2024-01-01\n")
    with pytest.raises(AshareScopeError, match="alias"):
        HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")


@pytest.mark.parametrize("identity", ["BJ920001", "SH510300", "SZ200001", "sz300001", "300001", "SHT00018"])
def test_canonical_csi1000_a_share_security_identity_is_required(tmp_path, identity):
    paths = current_files(tmp_path)
    revise(paths["csi1000"], lambda value: value["members"].__setitem__(0, identity))
    with pytest.raises(AshareScopeError, match="canonical SH/SZ"):
        CurrentAshareScope.from_files(paths, as_of_date=DAY)


def test_current_source_date_and_fetch_time_are_distinct_and_future_membership_is_unknown(tmp_path):
    paths = current_files(tmp_path)
    scope = CurrentAshareScope.from_files(paths, as_of_date=DAY)
    view = scope.view_on(DAY)
    assert view["as_of_date"] == "2026-10-19" and view["observed_at"]["csi300"] == FETCHED
    assert view["member_count"] == 1300 and not view["historical_signal_availability_proven"]
    for unknown in ("2026-10-18", "2026-10-20"):
        with pytest.raises(AshareScopeError, match="cannot be backdated or carried"):
            scope.members_on(unknown)


@pytest.mark.parametrize("mutation,error", [
    (lambda value: value["members"].pop(), "count_999_expected_1000"),
    (lambda value: value["members"].append(value["members"][0]), "duplicate"),
    (lambda value: value["members"].__setitem__(0, symbols()["csi300"][0]), "cross_index_duplicate"),
    (lambda value: value.update(source_evidence=[]), "original dated source evidence"),
    (lambda value: value.update(as_of_date="2026-10-20"), "index/date differs"),
    (lambda value: value.update(scope="csi500"), "index/date differs"),
    (lambda value: value.update(fetched_at="2026-10-18T23:00:00+08:00"), "cannot predate"),
    (lambda value: value.update(fetched_at="2026-10-20T09:59:27"), "timezone-aware"),
])
def test_current_incomplete_or_ambiguous_observation_fails_closed(tmp_path, mutation, error):
    paths = current_files(tmp_path)
    revise(paths["csi1000"], mutation)
    with pytest.raises(AshareScopeError, match=error):
        CurrentAshareScope.from_files(paths, as_of_date=DAY)


def test_current_union_needs_both_original_sources_and_their_unchanged_bytes(tmp_path):
    paths = current_files(tmp_path)
    with pytest.raises(AshareScopeError, match="Both original"):
        CurrentAshareScope.from_files({"csi300": paths["csi300"]}, as_of_date=DAY)
    scope = CurrentAshareScope.from_files(paths, as_of_date=DAY)
    raw = tmp_path / "csi1000-official-source.xls"
    raw.write_bytes(b"silently replaced raw source")
    with pytest.raises(AshareScopeError, match="changed after binding"):
        scope.members_on(DAY)


def test_filter_is_a_view_and_does_not_modify_full_source_or_cached_security_list(tmp_path):
    paths = historical_files(tmp_path)
    before = {name: path.read_bytes() for name, path in paths.items()}
    scope = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    cache = symbols()["csi300"] + symbols()["csi1000"] + ["BJ920001", "SH510300"]
    copy = list(cache)
    filtered = scope.filter_cached_instruments(cache, "2023-01-03")
    assert len(filtered) == 1300 and "BJ920001" not in filtered and "SH510300" not in filtered
    assert cache == copy and all(paths[name].read_bytes() == content for name, content in before.items())


@pytest.mark.parametrize("purpose", ["sell", "valuation"])
def test_existing_out_of_scope_position_is_manageable_even_if_source_disappears(tmp_path, purpose):
    paths = current_files(tmp_path)
    scope = CurrentAshareScope.from_files(paths, as_of_date=DAY)
    held = "SZ002999"
    paths["csi1000"].unlink()
    assert scope.access_allowed(held, "2026-10-20", purpose=purpose, held_instruments=[held])
    assert access_allowed(None, instrument=held, session="2026-10-20", purpose=purpose, held_instruments=[held])
    assert not access_allowed(None, instrument=held, session="2026-10-20", purpose=purpose, held_instruments=[])


def test_only_currently_in_scope_new_buys_are_allowed_and_existing_holdings_cannot_bypass(tmp_path):
    paths = current_files(tmp_path)
    scope = CurrentAshareScope.from_files(paths, as_of_date=DAY)
    assert scope.access_allowed(symbols()["csi300"][0], DAY, purpose="new_buy")
    assert scope.access_allowed(symbols()["csi1000"][0], DAY, purpose="new_buy")
    assert not scope.access_allowed("SZ002999", DAY, purpose="new_buy", held_instruments=["SZ002999"])
    assert not access_allowed(None, instrument="SZ002999", session=DAY, purpose="new_buy")
    with pytest.raises(AshareScopeError, match="cannot be backdated or carried"):
        scope.access_allowed(symbols()["csi300"][0], "2026-10-20", purpose="new_buy")


def project_declaration(tmp_path):
    from stopat30m.data.ashare_scope import DEFAULT_SCOPE_DECLARATION
    path = tmp_path / "project-scope.json"
    path.write_bytes(DEFAULT_SCOPE_DECLARATION.read_bytes())
    return path


def test_project_download_targets_and_training_rows_are_dated_views_not_a_constant_union(tmp_path):
    import pandas as pd
    from stopat30m.data.ashare_scope import ProjectAshareScope
    paths = historical_files(tmp_path, migrate=True)
    historical = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    project = ProjectAshareScope.from_file(project_declaration(tmp_path), historical=historical)
    sessions = ["2023-06-01", "2024-06-03"]
    targets = project.download_targets("2023-01-01", "2024-12-31", sessions=sessions)
    assert len(targets) == 1300 and "SZ002999" not in targets
    large, small = symbols()["csi300"][0], symbols()["csi1000"][0]
    frame = pd.DataFrame({"datetime": ["2023-06-01", "2023-06-01", "2024-06-03", "2024-06-03", "2024-06-03"],
        "instrument": [large, small, large, small, "SZ002999"], "feature": [1, 2, 3, 4, 5]})
    before = frame.copy(deep=True)
    selected = project.filter_training_frame(frame, scope="csi300")
    assert selected.feature.tolist() == [1, 4]  # Each index's actual dated membership.
    assert len(project.filter_training_frame(frame)) == 4  # Union still excludes unrelated stock.
    pd.testing.assert_frame_equal(frame, before)
    indexed = frame.set_index(["datetime", "instrument"])
    assert project.filter_training_frame(indexed, scope="csi300").feature.tolist() == [1, 4]


@pytest.mark.parametrize("mutate", [
    lambda value: value["allowed_indices"].update(csi500={}),
    lambda value: value["allowed_indices"].update(all={}),
    lambda value: value.update(outside_scope_new_training_samples=True),
    lambda value: value.update(outside_scope_new_download_targets=True),
    lambda value: value.update(original_a3_and_a4_protocols_are_not_rewritten=False),
])
def test_project_declaration_cannot_expand_scope_or_relabel_legacy_validation(tmp_path, mutate):
    from stopat30m.data.ashare_scope import ProjectAshareScope
    path = project_declaration(tmp_path)
    revise(path, mutate)
    with pytest.raises(AshareScopeError):
        ProjectAshareScope.from_file(path)


def test_current_snapshot_cannot_replace_required_historical_training_evidence(tmp_path):
    import pandas as pd
    from stopat30m.data.ashare_scope import ProjectAshareScope
    current = CurrentAshareScope.from_files(current_files(tmp_path), as_of_date=DAY)
    project = ProjectAshareScope.from_file(project_declaration(tmp_path), current=current)
    with pytest.raises(AshareScopeError, match="current members cannot backfill training"):
        project.filter_training_frame(pd.DataFrame({"datetime": ["2023-06-01"], "instrument": [symbols()["csi300"][0]]}))
    with pytest.raises(AshareScopeError, match="Historical scope evidence"):
        project.download_targets("2023-01-01", "2024-12-31", sessions=["2023-06-01"])


def test_project_declaration_change_blocks_new_samples_and_buys_but_never_locks_held_exit(tmp_path):
    from stopat30m.data.ashare_scope import ProjectAshareScope
    paths = historical_files(tmp_path)
    historical = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    declaration = project_declaration(tmp_path)
    project = ProjectAshareScope.from_file(declaration, historical=historical)
    revise(declaration, lambda value: value.update(default_scope="all"))
    with pytest.raises(AshareScopeError, match="changed after binding"):
        project.download_targets("2023-01-01", "2024-12-31", sessions=["2023-06-01"])
    with pytest.raises(AshareScopeError, match="changed after binding"):
        project.access_allowed(symbols()["csi300"][0], "2023-06-01", purpose="new_buy")
    assert project.access_allowed("SZ002999", "2023-06-01", purpose="sell", held_instruments=["SZ002999"])


@pytest.mark.parametrize("blocked", [False, True])
def test_scope_receipt_filters_without_copying_or_mutating_the_broad_source(tmp_path, monkeypatch, blocked):
    from types import SimpleNamespace
    from stopat30m.data import qlib_snapshot
    from stopat30m.data.ashare_scope import build_scope_receipt
    snapshot = tmp_path / "synthetic-snapshot"
    dataset = snapshot / "dataset"
    intervals = dataset / "instruments"
    intervals.mkdir(parents=True)
    historical_files(intervals, migrate=True)
    if blocked:
        path = intervals / "csi1000.txt"
        path.write_text(path.read_text() + "SZ302132\t2023-01-01\t2024-12-31\n")
    (snapshot / "manifest.json").write_text('{"explicit_synthetic_fixture":true}')
    calendar = dataset / "calendars"
    calendar.mkdir()
    (calendar / "day.txt").write_text("2023-06-01\n2024-06-03\n")
    features = dataset / "features"
    features.mkdir()
    (features / symbols()["csi300"][0].lower()).mkdir()
    (features / "bj920001").mkdir()
    current_dir = tmp_path / "current"
    current_dir.mkdir()
    current_files(current_dir)
    original = {str(path): path.read_bytes() for path in snapshot.rglob("*") if path.is_file()}
    monkeypatch.setattr(qlib_snapshot, "load_qlib_snapshot", lambda *args, **kwargs: SimpleNamespace(path=snapshot, dataset_dir=dataset))
    output = tmp_path / "scope-output"
    receipt = build_scope_receipt(snapshot, "2023-01-01", "2024-12-31", current_dir, output,
        declaration_path=project_declaration(tmp_path))
    assert receipt["historical_union_security_count"] == (1301 if blocked else 1300)
    assert receipt["current"]["member_count"] == 1300
    assert receipt["historical_daily_audit"]["status"] == ("blocked" if blocked else "passed")
    assert receipt["historical_cached_eligible_symbols"] == ([] if blocked else [symbols()["csi300"][0]])
    if blocked:
        assert receipt["historical_download_targets"] == []
        assert "SZ302132" in receipt["historical_unvalidated_source_candidates"]
    assert not receipt["returns_generated"] and not receipt["new_union_strategy_activated"]
    assert not receipt["source_datasets_modified"]
    assert all(Path(path).read_bytes() == content for path, content in original.items())
    assert not (output / "dataset").exists()
    manifest = json.loads((output / "manifest.json").read_text())
    assert all(digest(output / name) == value for name, value in manifest["files"].items())


def test_diagnostic_source_candidates_cannot_bypass_failed_collection_or_training_gate(tmp_path):
    import pandas as pd
    paths = historical_files(tmp_path)
    paths["csi1000"].write_text(paths["csi1000"].read_text() + "SZ302132\t2023-01-01\t2024-12-31\n")
    scope = HistoricalAshareScope.from_files(paths, start="2023-01-01", end="2024-12-31")
    candidates = scope.unvalidated_source_candidates("2023-01-01", "2024-12-31", sessions=["2023-01-03"])
    assert len(candidates) == 1301 and "SZ302132" in candidates
    with pytest.raises(AshareScopeError, match="count_1001"):
        scope.download_targets("2023-01-01", "2024-12-31", sessions=["2023-01-03"])
    with pytest.raises(AshareScopeError, match="count_1001"):
        scope.filter_training_frame(pd.DataFrame({"datetime": ["2023-01-03"], "instrument": ["SZ302132"]}))


def reviewed_code_event(tmp_path, *, effective="2023-07-01"):
    primary, reminder = tmp_path / "issuer-implementation.pdf", tmp_path / "issuer-reminder.pdf"
    primary.write_bytes(b"explicit synthetic code-validity implementation source")
    reminder.write_bytes(b"explicit synthetic independent issuer reminder")
    evidence = {"schema_version": 1, "kind": "ashare_reviewed_code_validity_evidence", "code_changes": [{
        "old_instrument": "SZ300114", "new_instrument": "SZ302132", "effective_date": effective,
        "same_legal_issuer": True, "share_quantity_changes": False, "source_evidence": [
            {"path": str(path), "sha256": digest(path), "url": "https://example.test/" + path.name} for path in (primary, reminder)]}]}
    path = tmp_path / "code-validity.json"
    path.write_text(json.dumps(evidence))
    return path


def test_evidenced_derivation_only_clips_code_dates_preserving_original_sources_and_other_rows(tmp_path):
    from stopat30m.data.ashare_scope import derive_code_validity_intervals
    paths = historical_files(tmp_path)
    paths["csi1000"].write_text(paths["csi1000"].read_text() + "SZ302132\t2023-01-01\t2024-12-31\n")
    original = {name: path.read_bytes() for name, path in paths.items()}
    evidence = reviewed_code_event(tmp_path)
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("2023-01-03\n2023-06-30\n2023-07-03\n2024-01-02\n")
    output = tmp_path / "derived"
    report = derive_code_validity_intervals(paths, evidence, calendar, output, start="2023-01-01", end="2024-12-31")
    assert report["status"] == "passed"
    assert not report["price_data_modified"] and not report["execution_data_modified"]
    assert all(paths[name].read_bytes() == payload for name, payload in original.items())
    assert (output / "csi300.txt").read_bytes() == original["csi300"]
    derived = HistoricalAshareScope.from_derived(output, start="2023-01-01", end="2024-12-31")
    assert "SZ300114" in derived.members_on("2023-06-30") and "SZ302132" not in derived.members_on("2023-06-30")
    assert "SZ302132" in derived.members_on("2023-07-03") and "SZ300114" not in derived.members_on("2023-07-03")
    assert all(row["derived_count"] == 1000 and row["original_count"] == 1001 for row in report["session_membership_differences"])
    assert all(not row["added"] for row in report["session_membership_differences"])
    assert "no_price_or_issuer_mapping" in derived.view_on("2023-06-30")["membership_basis"]


def test_future_code_duplicate_removed_only_before_actual_effective_date(tmp_path):
    from stopat30m.data.ashare_scope import derive_code_validity_intervals
    paths = historical_files(tmp_path)
    paths["csi1000"].write_text(paths["csi1000"].read_text() + "SZ302132\t2023-01-01\t2024-12-31\n")
    evidence = reviewed_code_event(tmp_path, effective="2025-02-17")
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("2023-01-03\n2024-01-02\n")
    output = tmp_path / "derived"
    report = derive_code_validity_intervals(paths, evidence, calendar, output, start="2023-01-01", end="2024-12-31")
    assert report["status"] == "passed"
    assert len(report["edits"]) == 1 and report["edits"][0]["derived_row"] is None
    assert all(row["removed"] == ["SZ302132"] for row in report["session_membership_differences"])
    derived = HistoricalAshareScope.from_derived(output, start="2023-01-01", end="2024-12-31")
    assert len(derived.members_on("2023-01-03")) == 1300
    source = Path(json.loads(evidence.read_text())["code_changes"][0]["source_evidence"][0]["path"])
    source.write_bytes(b"silently revised issuer dates")
    with pytest.raises(AshareScopeError, match="checksum differs"):
        HistoricalAshareScope.from_derived(output, start="2023-01-01", end="2024-12-31")


def test_missing_issuer_evidence_cannot_derive_a_convenient_pool_by_observed_returns(tmp_path):
    from stopat30m.data.ashare_scope import derive_code_validity_intervals
    paths = historical_files(tmp_path)
    evidence = reviewed_code_event(tmp_path)
    revise(evidence, lambda value: value["code_changes"][0].update(source_evidence=[]))
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("2023-01-03\n2024-01-02\n")
    output = tmp_path / "never-created"
    with pytest.raises(AshareScopeError, match="independently evidenced"):
        derive_code_validity_intervals(paths, evidence, calendar, output, start="2023-01-01", end="2024-12-31")
    assert not output.exists()
