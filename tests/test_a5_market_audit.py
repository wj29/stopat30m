"""A5 must stop on source conflicts, impossible members and unknown tax facts."""
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from stopat30m.data import a5_market_audit as audit


A = "SH600000"
B = "SZ000001"


def frame(*, instrument=A, close="10", status="1", date="2024-01-02"):
    return pd.DataFrame([{"instrument": instrument, "date": date,
        "code": instrument[:2].lower() + "." + instrument[2:],
        "open": "10", "high": "10", "low": "10", "close": close,
        "preclose": "10", "volume": "100", "amount": "1000", "adjustflag": "3",
        "tradestatus": status, "isST": "0", "source_complete_quote": status == "1",
        "source_response_sha256": "fixture-source", "source_fetched_at": "2026-10-20T00:00:00+00:00"}])


def basic(*, instrument=A, ipo="1999-11-10", out="", stock_type="1"):
    return pd.DataFrame([{"code": instrument[:2].lower() + "." + instrument[2:],
                          "ipoDate": ipo, "outDate": out, "type": stock_type}])


def test_overlapping_members_deduplicate_only_identical_actual_daily_terms():
    left, right = frame(), frame()
    right["source_response_sha256"] = "second-collection-bytes"
    right["source_fetched_at"] = "2026-10-20T02:00:00+00:00"
    merged, identities = audit.merge_execution_frames([left, right], [basic(), basic()])
    assert len(merged) == len(identities) == 1


@pytest.mark.parametrize("field,value", [("close", "10.01"), ("tradestatus", "0"),
    ("isST", "1"), ("preclose", "9.99"), ("volume", "101")])
def test_conflicting_collection_prices_status_or_volume_cannot_be_arbitrarily_selected(field, value):
    right = frame()
    right[field] = value
    with pytest.raises(ValueError, match="Conflicting original market"):
        audit.merge_execution_frames([frame(), right], [basic(), basic()])


def test_current_basic_identity_changes_are_not_silently_unioned():
    with pytest.raises(ValueError, match="Conflicting original IPO/delisting"):
        audit.merge_execution_frames([frame(), frame()], [basic(), basic(out="2024-08-28")])


def test_dividend_duplicate_source_generation_does_not_duplicate_entitlement():
    event = {"event_id": "economic-event", "instrument": A, "ex_date": "2024-06-20",
             "cash_per_share": ".1", "known_at": "2024-06-01T23:59:59+08:00"}
    duplicate = {**event, "generation": "another-fetch", "source_response_sha256": "different-byte-hash"}
    assert audit.merge_dividend_events([[event], [duplicate]]) == [event]
    with pytest.raises(ValueError, match="Conflicting duplicate dividend"):
        audit.merge_dividend_events([[event], [{**duplicate, "cash_per_share": ".2"}]])


class Scope:
    def __init__(self, members=(A,)):
        self.members = list(members)

    def view_on(self, day, *, scope="union"):
        return {"members": self.members, "member_count": len(self.members),
                "index_counts": {"csi300": 300, "csi1000": 1000}}


def member_audit(raw=None, identity=None, facts=()):
    return audit.audit_member_identity(frame() if raw is None else raw,
        basic() if identity is None else identity, Scope(), ["2024-01-02"],
        start="2024-01-01", end="2024-12-31", identity_events=facts)


@pytest.mark.parametrize("identity", [basic(ipo="2024-01-03"), basic(out="2024-01-02"),
                                      basic(stock_type="2"), basic(instrument=B)])
def test_member_cardinality_cannot_bless_prelisting_delisted_or_unknown_identity(identity):
    assert member_audit(identity=identity)["status"] == "blocked"


def test_explicit_suspended_member_is_still_listed_with_a_source_status():
    result = member_audit(raw=frame(status="0"))
    assert result["status"] == "passed"
    assert member_audit(raw=frame().iloc[:0])["failures"][0]["reason"] == "member_has_no_original_daily_status"


def test_delisting_requires_official_transition_fact_even_if_no_longer_member():
    identity = basic(out="2024-08-28")
    result = member_audit(identity=identity)
    assert result["status"] == "blocked"
    assert result["failures"][-1]["reason"] == "terminal_identity_requires_official_accounting_evidence"
    fact = {"instrument": A, "effective_date": "2024-08-28", "kind": "delisting"}
    assert member_audit(identity=identity, facts=[fact])["status"] == "passed"


def test_event_facts_require_unchanged_original_bytes(tmp_path):
    source = tmp_path / "original.pdf"
    source.write_bytes(b"synthetic original public fact")
    payload = {"events": [{"instrument": A, "ex_date": "2024-06-20", "evidence": [
        {"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}]}]}
    path = tmp_path / "facts.json"
    path.write_text(json.dumps(payload))
    loaded, proofs = audit.load_event_evidence([path], key_fields=("instrument", "ex_date"))
    assert len(loaded["events"]) == 1 and len(proofs) == 2
    source.write_bytes(b"changed source bytes")
    with pytest.raises(ValueError, match="source bytes changed"):
        audit.load_event_evidence([path], key_fields=("instrument", "ex_date"))


def test_original_fact_without_evidence_or_with_relative_source_is_not_accepted(tmp_path):
    path = tmp_path / "facts.json"
    path.write_text(json.dumps({"events": [{"instrument": A, "ex_date": "2024-06-20"}]}))
    with pytest.raises(ValueError, match="original source evidence"):
        audit.load_event_evidence([path], key_fields=("instrument", "ex_date"))


def test_supplementary_rendered_page_is_bound_and_changed_original_rejected(tmp_path):
    source = tmp_path / "issuer.pdf"
    source.write_bytes(b"original issuer source")
    image = tmp_path / "issuer_p1.png"
    image.write_bytes(b"original rendered page")
    payload = {"events": [{"instrument": A, "ex_date": "2024-06-20", "evidence": [
        {"path": str(source), "sha256": audit.sha(source)}], "visual_review_evidence": [
        {"path": str(image), "sha256": audit.sha(image)}]}]}
    path = tmp_path / "facts.json"
    path.write_text(json.dumps(payload))
    _, proofs = audit.load_event_evidence([path], key_fields=("instrument", "ex_date"))
    assert any(p["path"] == str(image) for p in proofs.values())
    image.write_bytes(b"different page")
    with pytest.raises(ValueError, match="Supplementary original event source bytes changed"):
        audit.load_event_evidence([path], key_fields=("instrument", "ex_date"))
    path.write_text(json.dumps({"events": [{"instrument": A, "ex_date": "2024-06-20",
        "evidence": [{"path": "relative.pdf", "sha256": "irrelevant"}]}]}))
    with pytest.raises(ValueError, match="absolute original paths"):
        audit.load_event_evidence([path], key_fields=("instrument", "ex_date"))


def test_price_bridge_requires_finite_positive_factor_and_exact_fixed_tolerance(monkeypatch):
    stored = pd.DataFrame([[20., 20., 20., 20., 1., 20., 2.]], index=["2024-01-02"],
                          columns=["open", "high", "low", "close", "volume", "vwap", "factor"])
    monkeypatch.setattr(audit, "read_qlib_security", lambda *args: stored)
    assert audit.audit_raw_qlib_bridge("fixture", frame(), [A], start="2024-01-01", end="2024-12-31")["status"] == "passed"
    stored.loc[:, "factor"] = 0.
    result = audit.audit_raw_qlib_bridge("fixture", frame(), [A], start="2024-01-01", end="2024-12-31")
    assert result["status"] == "blocked"
    assert result["failures"][0]["reason"] == "observed_source_missing_finite_model_feature_fields"
    assert result["failures"][0]["missing_fields"] == ["factor"]


def test_price_bridge_cannot_skip_trading_rows_missing_from_model_calendar(monkeypatch):
    stored = pd.DataFrame([[10., 10., 10., 10., 1., 10., 1.]], index=["2024-01-03"],
                          columns=["open", "high", "low", "close", "volume", "vwap", "factor"])
    monkeypatch.setattr(audit, "read_qlib_security", lambda *args: stored)
    result = audit.audit_raw_qlib_bridge("fixture", frame(), [A], start="2024-01-01", end="2024-12-31")
    assert result["status"] == "blocked"
    assert result["failures"][0]["reason"] == "trading_source_outside_model_calendar"


@pytest.mark.parametrize("field", ["volume", "vwap"])
def test_price_bridge_rejects_missing_nonprice_model_features_even_when_all_ohlc_match(monkeypatch, field):
    stored = pd.DataFrame([[10., 10., 10., 10., 1., 10., 1.]], index=["2024-01-02"],
                          columns=["open", "high", "low", "close", "volume", "vwap", "factor"])
    stored.loc[:, field] = np.nan
    monkeypatch.setattr(audit, "read_qlib_security", lambda *args: stored)
    result = audit.audit_raw_qlib_bridge("fixture", frame(), [A], start="2024-01-01", end="2024-12-31")
    assert result["status"] == "blocked" and result["failures"][0]["missing_fields"] == [field]


def test_unknown_capital_tax_and_positive_volume_halt_are_review_items_not_assumptions():
    event = {"event_id": "capital", "instrument": A, "ex_date": "2024-06-20",
             "capital_reserve_ratio": ".2", "stock_dividend_ratio": "0"}
    raw = frame(status="0")
    raw["source_observed_market_quote"] = False
    result = audit.source_review_queue(raw, [event], {"unexplained": []},
        capital_evidence={"events": []}, identity_audit={"failures": []},
        start="2024-01-01", end="2024-12-31")
    assert result["item_count"] == 2
    assert result["items"][0]["reasons"] == ["share_premium_or_explicit_issuer_capital_tax_evidence"]
    assert result["items"][1]["reasons"] == ["official_partial_session_suspension_and_independent_quote"]
    assert result["portfolio_returns_computed"] is False


def original_collectors(tmp_path, monkeypatch, *, targets=(A,), years=(2024,),
                        source_start="2023-12-29", source_end="2024-12-31"):
    """Collector decoding is separately tested; exercise union gate/proof binding."""
    execution, dividends = tmp_path / "execution", tmp_path / "dividends"
    execution.mkdir(); dividends.mkdir()
    selection = tmp_path / "selection.txt"
    selection.write_text("explicit synthetic source interval evidence")
    for directory, filename in ((execution, "execution_collection.json"),
                                 (dividends, "collection.json")):
        (directory / filename).write_text(json.dumps({"kind": "synthetic immutable manifest"}))
        (directory / "response.json").write_text("original response bytes")
    response = {"file": "response.json", "sha256": hashlib.sha256((execution / "response.json").read_bytes()).hexdigest()}
    receipt = {"request": {"start_date": source_start, "end_date": source_end,
        "instruments": list(targets), "selection_evidence_path": str(selection)},
        "responses": {A: response}}
    (dividends / "response.json").write_text(json.dumps({"request": {
        "code": "sh.600000", "year": "2024", "yearType": "operate"},
        "fields": ["code", "dividOperateDate"], "rows": [["sh.600000", "2024-06-20"]]}))
    dividend_response = {"file": "response.json", "sha256": hashlib.sha256((dividends / "response.json").read_bytes()).hexdigest()}
    dividend_receipt = {"years": list(years), "instruments": list(targets), "responses": {A + "_2024": dividend_response}}
    monkeypatch.setattr(audit, "load_execution_collection", lambda *args: (frame(), basic(), receipt))
    monkeypatch.setattr(audit, "load_dividend_evidence", lambda *args: (pd.DataFrame(), dividend_receipt))
    monkeypatch.setattr(audit, "dividend_events", lambda *args, **kwargs: [])

    class FullScope(Scope):
        def download_targets(self, *args, **kwargs):
            return self.members

        def audit_sessions(self, *args, **kwargs):
            return {"status": "passed"}

        def assert_unchanged(self):
            pass

    return execution, dividends, FullScope(), receipt, dividend_receipt


def test_loader_requires_complete_union_and_binds_each_source_original(tmp_path, monkeypatch):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch)
    result = audit.load_a5_market_evidence([execution], [dividends], scope=scope,
        calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31")
    assert result.instruments == [A]
    assert result.source_proofs["execution_0_original_" + A]["path"] == str(execution / "response.json")
    assert result.source_proofs["dividend_0_original_" + A + "_2024"]["path"] == str(dividends / "response.json")
    assert result.source_proofs["execution_selection_0"]["path"].endswith("selection.txt")


def test_dividend_request_label_cannot_hide_stale_server_year_after_transport_failure(tmp_path):
    source = tmp_path / "original.json"
    source.write_text(json.dumps({"request": {"code": "sh.600000", "year": "2024", "yearType": "operate"},
        "fields": ["code", "dividOperateDate"], "rows": [["sh.600000", "2023-06-20"]]}))
    with pytest.raises(ValueError, match="server security/operate year"):
        audit.audit_original_dividend_request(source, A + "_2024")


def test_empty_dividend_response_does_not_pretend_to_echo_server_operate_year(tmp_path):
    source = tmp_path / "empty.json"
    source.write_text(json.dumps({"request": {"code": "sh.600000", "year": "2024", "yearType": "operate"},
        "fields": ["code", "dividOperateDate"], "rows": []}))
    result = audit.audit_original_dividend_request(source, A + "_2024")
    assert result["row_count"] == 0 and result["empty_response_is_not_server_echo_proof"] is True


def test_scope_batch_exit_verification_cannot_be_bypassed_by_returning_identity_result():
    from contextlib import contextmanager

    class ChangedScope(Scope):
        @contextmanager
        def verified_batch(self):
            yield
            raise ValueError("Original membership source changed on exit")

        def view_on(self, day, *, scope="union", include_bound_inputs=True):
            assert include_bound_inputs is False
            return super().view_on(day, scope=scope)

    with pytest.raises(ValueError, match="source changed on exit"):
        audit.audit_member_identity(frame(), basic(), ChangedScope(), ["2024-01-02"],
            start="2024-01-01", end="2024-12-31")


@pytest.mark.parametrize("source_start,source_end", [("2024-01-01", "2024-12-31"),
                                                     ("2023-12-29", "2024-12-30")])
def test_loader_does_not_invent_a_preceding_close_or_extend_source_end(tmp_path, monkeypatch, source_start, source_end):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch,
        source_start=source_start, source_end=source_end)
    with pytest.raises(ValueError, match="preceding close or audit end"):
        audit.load_a5_market_evidence([execution], [dividends], scope=scope,
            calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31")


def test_loader_requires_dividend_year_coverage_for_all_targets(tmp_path, monkeypatch):
    execution, dividends, scope, _, div_receipt = original_collectors(tmp_path, monkeypatch)
    div_receipt["years"] = [2023]
    with pytest.raises(ValueError, match="Dividend collection years/targets"):
        audit.load_a5_market_evidence([execution], [dividends], scope=scope,
            calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31")


def test_loader_cannot_publish_a_subset_as_the_complete_union(tmp_path, monkeypatch):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch)
    scope.members.append(B)
    with pytest.raises(ValueError, match="cover every historical union target"):
        audit.load_a5_market_evidence([execution], [dividends], scope=scope,
            calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31")


def test_loader_never_uses_raw_cardinality_to_override_a_failed_membership_audit(tmp_path, monkeypatch):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch)
    monkeypatch.setattr(scope, "audit_sessions", lambda *args, **kwargs: {"status": "blocked"})
    with pytest.raises(ValueError, match="membership failed"):
        audit.load_a5_market_evidence([execution], [dividends], scope=scope,
            calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31")


def test_existing_archive_superset_requires_explicit_scope_view_and_preserves_all_original_proofs(tmp_path, monkeypatch):
    execution, dividends, scope, receipt, div_receipt = original_collectors(tmp_path, monkeypatch, targets=(A, B))
    archived = pd.concat([frame(), frame(instrument=B)], ignore_index=True)
    registry = pd.concat([basic(), basic(instrument=B)], ignore_index=True)
    monkeypatch.setattr(audit, "load_execution_collection", lambda *args: (archived, registry, receipt))
    a_event = {"event_id": "a", "instrument": A, "ex_date": "2024-06-20"}
    b_event = {"event_id": "b", "instrument": B, "ex_date": "2024-06-20"}
    monkeypatch.setattr(audit, "dividend_events", lambda *args, **kwargs: [a_event, b_event])
    kwargs = dict(scope=scope, calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31")
    with pytest.raises(ValueError, match="exceed the permitted"):
        audit.load_a5_market_evidence([execution], [dividends], **kwargs)
    loaded = audit.load_a5_market_evidence([execution], [dividends], archive_scope_view=True, **kwargs)
    assert list(loaded.raw.instrument) == [A] and loaded.events == [a_event]
    assert len(loaded.basic) == 2  # Identity archive retained, never a buy universe.
    view = loaded.archive_scope_view_receipt
    assert view["excluded_input_instruments"] == [B]
    assert view["excluded_input_record_count"] == view["excluded_input_event_count"] == 1
    assert view["source_vintage"] == "original_archive_generations_preserved_not_new_supplier_collections"
    assert "execution_collection_0" in loaded.source_proofs and "execution_0_original_" + A in loaded.source_proofs


def test_explicit_archive_filter_still_requires_all_official_targets(tmp_path, monkeypatch):
    execution, dividends, scope, receipt, div_receipt = original_collectors(tmp_path, monkeypatch)
    scope.members.append(B)
    with pytest.raises(ValueError, match="cover every historical union target"):
        audit.load_a5_market_evidence([execution], [dividends], scope=scope,
            calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31", archive_scope_view=True)


def test_unrelated_registry_delisting_does_not_create_an_outside_scope_execution_event():
    identity = pd.concat([basic(), basic(instrument=B, out="2024-08-28")], ignore_index=True)
    assert member_audit(identity=identity)["status"] == "passed"


def test_count_correct_scope_without_official_effective_history_is_still_blocked(tmp_path):
    result, proofs = audit.audit_official_membership_evidence(None, tmp_path,
        start="2023-01-01", end="2024-12-31")
    assert result["status"] == "blocked" and not proofs
    assert not any(result["checks"].values())


def test_official_membership_requires_all_named_checks_and_unchanged_originals(tmp_path):
    scope = tmp_path / "scope"
    scope.mkdir()
    (scope / "manifest.json").write_text("exact derived scope bytes")
    original = tmp_path / "official-notice.pdf"
    original.write_bytes(b"synthetic official original")
    document = tmp_path / "official-history.json"
    payload = {"kind": "a5_official_membership_evidence", "status": "passed",
        "start": "2023-01-01", "end": "2024-12-31",
        "historical_scope_manifest_sha256": hashlib.sha256((scope / "manifest.json").read_bytes()).hexdigest(),
        "checks": {key: True for key in audit.OFFICIAL_MEMBERSHIP_CHECKS},
        "bound_inputs": {"nested": [{"path": str(original), "sha256": hashlib.sha256(original.read_bytes()).hexdigest()}]}}
    document.write_text(json.dumps(payload))
    result, proofs = audit.audit_official_membership_evidence(document, scope,
        start="2023-01-01", end="2024-12-31")
    assert result["status"] == "passed" and len(proofs) == 2
    payload["checks"]["all_temporary_adjustments"] = "true"
    document.write_text(json.dumps(payload))
    assert audit.audit_official_membership_evidence(document, scope,
        start="2023-01-01", end="2024-12-31")[0]["status"] == "blocked"
    original.write_bytes(b"different original")
    with pytest.raises(ValueError, match="original bytes changed"):
        audit.audit_official_membership_evidence(document, scope,
            start="2023-01-01", end="2024-12-31")


def test_capital_tax_fact_for_same_date_cannot_bless_different_executed_terms(tmp_path):
    original = tmp_path / "issuer.pdf"
    original.write_bytes(b"synthetic premium source")
    event = {"event_id": "capital", "instrument": A, "ex_date": "2024-06-20",
        "capital_reserve_ratio": ".4", "cash_per_share": ".1", "stock_dividend_ratio": "0"}
    fact = {**event, "capital_reserve_tax_exempt": True, "status": "verified", "review_required": False,
        "evidence": [{"path": str(original), "sha256": hashlib.sha256(original.read_bytes()).hexdigest()}]}
    assert audit.audit_a5_capital_reserve_tax([event], {"events": [fact]}) == []
    assert audit.audit_a5_capital_reserve_tax([event], {"events": [{**fact, "capital_reserve_ratio": ".3"}]}) == ["capital"]
    assert audit.audit_a5_capital_reserve_tax([event], {"events": [{**fact, "status": "pending"}]}) == ["capital"]


def test_archive_scope_view_records_excluded_legacy_corrections_but_still_checks_their_originals(tmp_path, monkeypatch):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch)
    original = tmp_path / "legacy_issuer.pdf"
    original.write_bytes(b"synthetic outside official universe evidence")
    corrections = tmp_path / "legacy_patches.json"
    corrections.write_text(json.dumps({"patches": [{"operation": "annotate", "instrument": B,
        "ex_date": "2024-06-20", "values": {"cash_dividend_tax_rate": ".2"}, "evidence": {
            "path": original.name, "sha256": hashlib.sha256(original.read_bytes()).hexdigest(),
            "url": "https://example.org/official.pdf"}}]}))
    kwargs = dict(scope=scope, calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31",
                  action_overrides_paths=[corrections])
    with pytest.raises(ValueError, match="corrections exceed"):
        audit.load_a5_market_evidence([execution], [dividends], **kwargs)
    result = audit.load_a5_market_evidence([execution], [dividends], archive_scope_view=True, **kwargs)
    receipt = result.archive_scope_view_receipt["action_override_inputs"][0]
    assert receipt["consumed_patch_count"] == 0 and receipt["excluded_patch_count"] == 1
    assert result.source_proofs["action_override_0_original_0"]["path"] == str(original)
    original.write_bytes(b"tampered excluded original")
    with pytest.raises(ValueError, match="correction evidence changed"):
        audit.load_a5_market_evidence([execution], [dividends], archive_scope_view=True, **kwargs)


def test_stock_face_supporting_original_cannot_change_behind_unchanged_patch_document(tmp_path, monkeypatch):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch)
    original = tmp_path / "implementation.pdf"
    original.write_bytes(b"synthetic actual distribution")
    nominal = tmp_path / "ipo_face.pdf"
    nominal.write_bytes(b"synthetic actual nominal face source")
    corrections = tmp_path / "stock_patches.json"
    corrections.write_text(json.dumps({"patches": [{"operation": "annotate", "instrument": B,
        "ex_date": "2024-06-20", "values": {"cash_dividend_tax_rate": ".6"}, "evidence": {
            "path": str(original), "sha256": hashlib.sha256(original.read_bytes()).hexdigest(),
            "url": "https://example.org/implementation.pdf"}, "supporting_evidence": [{
            "path": str(nominal), "sha256": hashlib.sha256(nominal.read_bytes()).hexdigest(),
            "url": "https://example.org/ipo.pdf"}]}]}))
    kwargs = dict(scope=scope, calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31",
                  action_overrides_paths=[corrections], archive_scope_view=True)
    result = audit.load_a5_market_evidence([execution], [dividends], **kwargs)
    assert result.source_proofs["action_override_0_supporting_0_0"]["path"] == str(nominal)
    nominal.write_bytes(b"changed source face")
    with pytest.raises(ValueError, match="supporting action evidence changed"):
        audit.load_a5_market_evidence([execution], [dividends], **kwargs)


def test_later_override_cannot_silently_win_conflicting_verified_reference_values(tmp_path, monkeypatch):
    execution, dividends, scope, _, _ = original_collectors(tmp_path, monkeypatch)
    event = {"event_id": "actual", "instrument": A, "record_date": "2024-06-19", "ex_date": "2024-06-20",
        "pay_date": "2024-06-20", "listing_date": None, "cash_per_share": ".1", "bonus_ratio": "0",
        "stock_dividend_ratio": "0", "capital_reserve_ratio": "0", "known_at": "2024-06-01T23:59:59+08:00"}
    monkeypatch.setattr(audit, "dividend_events", lambda *args, **kwargs: [event])
    pdf = tmp_path / "issuer.pdf"
    pdf.write_bytes(b"synthetic reviewed source")
    paths = []
    for n, reference in enumerate([".09", ".08"]):
        path = tmp_path / f"reference_{n}.json"
        path.write_text(json.dumps({"patches": [{"operation": "annotate", "instrument": A, "ex_date": "2024-06-20",
            "values": {"reference_cash_per_share": reference}, "evidence": {"path": str(pdf),
                "sha256": hashlib.sha256(pdf.read_bytes()).hexdigest(), "url": "https://example.org/issuer.pdf"}}]}))
        paths.append(path)
    with pytest.raises(ValueError, match="Conflicting independently evidenced action"):
        audit.load_a5_market_evidence([execution], [dividends], scope=scope,
            calendar=["2023-12-29", "2024-01-02"], start="2024-01-01", end="2024-12-31",
            action_overrides_paths=paths)


def test_legacy_exemption_is_preserved_without_faking_new_visual_review_and_unknown_event_stays_pending(tmp_path, monkeypatch):
    source = tmp_path / "legacy_issuer.pdf"
    source.write_bytes(b"synthetic original legacy verified fact")
    event = {"event_id": "legacy", "instrument": A, "ex_date": "2024-06-20",
        "cash_per_share": ".1", "stock_dividend_ratio": "0", "capital_reserve_ratio": ".2"}
    fact = {**event, "status": "verified", "review_required": False, "capital_reserve_tax_exempt": True,
        "evidence": [{"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}]}
    unknown = {**event, "event_id": "unknown", "instrument": B}
    document = tmp_path / "legacy_facts.json"
    document.write_text(json.dumps({"events": [fact]}))
    monkeypatch.setattr(audit, "LEGACY_CAPITAL_FACT_DOCUMENT_SHA256", audit.sha(document))
    evidence, _ = audit.load_event_evidence([document], key_fields=("instrument", "ex_date"))
    adjusted, report = audit.apply_union_capital_tax([event, unknown], evidence)
    assert adjusted == [event, unknown]
    assert report["covered_event_ids"] == ["legacy"]
    assert report["pending"][0]["event_id"] == "unknown"
    assert report["original_exemption_evidence_not_relabelled_as_new_visual_review"] is True
    assert report["status"] == "blocked" and "original_pdf_visual_reviewed" not in fact


def test_new_exemption_cannot_impersonate_frozen_legacy_document_by_omitting_visual_fields(tmp_path):
    source = tmp_path / "new_issuer.pdf"
    source.write_bytes(b"synthetic new unchecked source")
    event = {"event_id": "new", "instrument": A, "ex_date": "2024-06-20",
        "cash_per_share": ".1", "stock_dividend_ratio": "0", "capital_reserve_ratio": ".2"}
    fact = {**event, "status": "verified", "review_required": False, "capital_reserve_tax_exempt": True,
        "evidence": [{"path": str(source), "sha256": audit.sha(source)}]}
    document = tmp_path / "new_facts.json"
    document.write_text(json.dumps({"events": [fact]}))
    evidence, _ = audit.load_event_evidence([document], key_fields=("instrument", "ex_date"))
    with pytest.raises(ValueError, match="reviewed original issuer PDF"):
        audit.apply_union_capital_tax([event], evidence)
