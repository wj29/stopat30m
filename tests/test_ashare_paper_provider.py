"""Explicit public-data provider contract; every network function is replaced."""
from datetime import datetime
import json
from pathlib import Path

import pytest

from stopat30m import ashare_forward as af
from stopat30m import ashare_paper_provider as module
from stopat30m.data import ashare_daily_update, ashare_actions, ashare_live
from stopat30m.data.research_calendar import Calendar


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return module._proof(path)


@pytest.fixture
def case(tmp_path, monkeypatch):
    clock = datetime.fromisoformat("2026-10-19T18:00:00+08:00")
    monkeypatch.setattr(module, "_now", lambda: clock)
    calendar = Calendar.from_rows([
        {"date": "2026-10-19", "is_open": True}, {"date": "2026-10-20", "is_open": True},
        {"date": "2026-10-21", "is_open": True}, {"date": "2026-10-22", "is_open": True},
        {"date": "2026-10-23", "is_open": True}, {"date": "2026-10-24", "is_open": False}],
        "2026-10-19", "2026-10-24", source="explicit_fixture", source_url="fixture://calendar")
    calendar_path = tmp_path / "calendar.json"
    calendar.save(calendar_path)
    paper = tmp_path / "paper"
    manifest = {"created_at": clock.isoformat()}
    manifest["manifest_sha256"] = af._digest(manifest)
    save(paper / "manifest.json", manifest)
    state = {"manifest_sha256": manifest["manifest_sha256"],
        "accounts": {"model": {"positions": {}, "claims": {}, "entitlements": {}}}}
    af._save_state(paper, state)
    raw = save(tmp_path / "source/raw.json", {"date": "2026-10-19", "raw": True})
    index = save(tmp_path / "source/index.json", {"date": "2026-10-19", "members": ["SH600000"]})
    snapshot = tmp_path / "snapshot"
    save(snapshot / "manifest.json", {"fixture": True})
    result = {"session": "2026-10-19", "snapshot_path": str(snapshot),
        "collection_path": str(tmp_path / "source"),
        "daily_quotes": {"SH600000": {"instrument": "SH600000", "session": "2026-10-19",
            "source": "baostock.daily_raw", "tradestatus": "1", "open": 10, "close": 10.5,
            "preclose": 10, "factor": .1, "fetched_at": "2026-10-19T16:00:00+08:00"}},
        "metadata": {"SH600000": {"instrument": "SH600000", "board": "sse_main", "listing_date": "1999-01-01",
            "out_date": None, "is_st": False, "status_session": "2026-10-19", "known_at": "2026-10-19T16:00:00+08:00"}},
        "current_index_instruments": ["SH600000"], "excluded": {},
        "source_evidence": {"raw_responses": {"SH600000": {"file": "raw.json", "sha256": raw["sha256"]}}},
        "universe_evidence": {"file": index["path"], "sha256": index["sha256"]}}
    initial = tmp_path / "bootstrap/refresh_result.json"
    save(initial, result)
    def loader(path):
        return json.loads(Path(path).read_text())
    monkeypatch.setattr(ashare_daily_update, "load_ashare_refresh", loader)
    adapter = {"initial_refresh_path": str(initial), "calendar_path": str(calendar_path),
        "provider_output_dir": str(tmp_path / "provider"), "dividend_overrides_path": None,
        "initial_dividend_collection": None}
    save(paper / "adapter.json", adapter)
    return {"paper": paper, "provider": module.AsharePaperProvider.from_paper_dir(paper),
        "result": result, "initial": initial, "tmp": tmp_path, "clock": clock, "monkeypatch": monkeypatch}


def test_bootstrap_close_retains_real_retrieval_and_only_declares_daily_completion(case, monkeypatch):
    monkeypatch.setattr(ashare_daily_update, "refresh_ashare_day", lambda *a, **k: pytest.fail("bootstrap must be reused"))
    result = case["provider"].fetch_close("2026-10-19", ["SH600000"], case["clock"])
    quote = result["quotes"]["SH600000"]
    assert quote["tradestatus"] == "trading" and quote["bar_complete"] is True
    assert "asof" not in quote and "as_of" not in quote
    assert quote["fetched_at"] == "2026-10-19T16:00:00+08:00"
    assert result["fetched_at"] == case["clock"].isoformat()
    assert result["metadata"]["SH600000"]["delisting_period"] is False
    assert any(Path(row["path"]).name == "raw.json" for row in result["evidence"])
    assert result["instruments"] == ["SH600000"] and not result["live_opening_quote"]


def test_next_close_uses_latest_verified_parent_and_passes_pending_targets(case, monkeypatch):
    clock = datetime.fromisoformat("2026-10-20T18:00:00+08:00")
    monkeypatch.setattr(module, "_now", lambda: clock)
    calls = []
    def refresh(parent, session, output, calendar, **options):
        calls.append((parent, session, options))
        result = {**case["result"], "session": session}
        save(output / session / "refresh_result.json", result)
        return result
    monkeypatch.setattr(ashare_daily_update, "refresh_ashare_day", refresh)
    case["provider"].fetch_close("2026-10-20", ["SH600000", "SZ000001"], clock)
    assert calls == [(case["result"]["snapshot_path"], "2026-10-20", {"extra_instruments": ["SH600000", "SZ000001"]})]


def test_live_open_samples_actual_receive_clock_and_current_st_only(case, monkeypatch):
    clock = datetime.fromisoformat("2026-10-20T09:35:30+08:00")
    monkeypatch.setattr(module, "_now", lambda: clock)
    captured = []
    def live(instruments, output, **options):
        captured.append(options)
        raw = save(output / "response_000.txt", {"public_observation_fixture": True})
        receipt = {"source": "tencent.public_level1_observation", "rejected": {}, "responses": [
            {"file": "response_000.txt", "sha256": raw["sha256"], "url": "https://qt.gtimg.cn/q=sh600000",
             "fetched_at": "2026-10-20T09:35:29+08:00", "instruments": instruments}],
            "quotes": {"SH600000": {"instrument": "SH600000", "session": "2026-10-20", "open": "10.00",
                "preclose": "10.50", "execution_price": "10.80", "source": "tencent.public_level1_observation",
                "asof": "2026-10-20T09:35:28+08:00", "tradestatus": "1", "is_st": True,
                "execution_price_basis": "live_observed_quote", "live_execution_eligible": True}}}
        save(output / "receipt.json", receipt)
        return receipt
    monkeypatch.setattr(ashare_live, "fetch_tencent_execution_quotes", live)
    result = case["provider"].fetch_open("2026-10-20", ["SH600000"], clock)
    assert captured[0]["now"] is None
    assert result["quotes"]["SH600000"]["execution_price"] == "10.80"
    assert result["quotes"]["SH600000"]["open"] == "10.00"
    assert result["quotes"]["SH600000"]["fetched_at"] == "2026-10-20T09:35:29+08:00"
    assert result["metadata"]["SH600000"]["is_st"] is True
    assert result["metadata"]["SH600000"]["status_session"] == "2026-10-20"
    assert result["metadata"]["SH600000"]["known_at"] == "2026-10-20T09:35:28+08:00"
    assert not result["guaranteed_fill"]


def test_calendar_and_bad_saved_bytes_fail_before_any_source(case, monkeypatch):
    monkeypatch.setattr(ashare_live, "fetch_tencent_execution_quotes", lambda *a, **k: pytest.fail("must stay offline"))
    monkeypatch.setattr(module, "_now", lambda: datetime.fromisoformat("2026-10-24T09:35:00+08:00"))
    with pytest.raises(ValueError, match="exchange session"):
        case["provider"].fetch_open("2026-10-24", ["SH600000"], None)
    monkeypatch.setattr(module, "_now", lambda: case["clock"])
    (case["tmp"] / "source/raw.json").write_text("changed")
    with pytest.raises(ValueError, match="evidence bytes"):
        case["provider"].fetch_close("2026-10-19", ["SH600000"], case["clock"])


def test_held_rights_refresh_dividend_originals_and_bind_override_files(case, monkeypatch):
    paper = case["paper"]
    state = json.loads((paper / "state.json").read_text())
    state["accounts"]["model"]["entitlements"] = {"pending": {"instrument": "SH600000", "status": "pending"}}
    af._save_state(paper, state)
    calls = []
    def collect(output, instruments, years, **options):
        calls.append((instruments, years))
        proof = save(output / "SH600000_2026.json", {"original_dividend_fixture": True})
        save(output / "collection.json", {"instruments": instruments, "responses": {
            "SH600000_2026": {"file": "SH600000_2026.json", "sha256": proof["sha256"]}}})
        return output
    monkeypatch.setattr(ashare_actions, "collect_dividend_evidence", collect)
    monkeypatch.setattr(ashare_actions, "load_dividend_evidence", lambda path: (None, json.loads((path / "collection.json").read_text())))
    def events(path, **kwargs):
        proof = module._proof(path / "SH600000_2026.json")
        return [{"instrument": "SH600000", "ex_date": "2026-10-19", "event_id": "fixture",
            "source_response_sha256": proof["sha256"]}]
    monkeypatch.setattr(ashare_actions, "dividend_events", events)
    result = case["provider"].fetch_close("2026-10-19", ["SH600000"], case["clock"])
    assert calls == [(["SH600000"], [2026])]
    event = result["corporate_actions"][0]
    assert event["source_response_sha256"] in {row["sha256"] for row in result["evidence"]}


def test_global_overrides_are_scoped_to_queried_events_and_actual_proofs(case, monkeypatch):
    initial = case["tmp"] / "initial-dividends"
    raw = save(initial / "raw.json", {"dividend_fixture_only": True})
    receipt = {"instruments": ["SH600000"], "responses": {"SH600000_2026": {"file": "raw.json", "sha256": raw["sha256"]}}}
    save(initial / "collection.json", receipt)
    provider = case["provider"]
    provider.initial_dividends = initial
    monkeypatch.setattr(ashare_actions, "load_dividend_evidence", lambda path: (None, receipt))
    event = {"event_id": "fixture-dividend", "instrument": "SH600000", "record_date": "2026-10-16",
        "ex_date": "2026-10-19", "pay_date": "2026-10-19", "listing_date": None,
        "cash_per_share": "0.20", "bonus_ratio": "0", "known_at": "2026-10-15T23:59:59+08:00",
        "source_response_sha256": raw["sha256"]}
    monkeypatch.setattr(ashare_actions, "dividend_events", lambda *a, **k: [event])
    proof = save(case["tmp"] / "official-virtual-reference.json", {"fixture_virtual_reference": ".19"})
    document = {"patches": [
        {"instrument": "SH600000", "ex_date": "2026-10-19", "operation": "annotate",
         "values": {"reference_cash_per_share": "0.19"},
         "evidence": {**proof, "url": "https://issuer.example/official-notice"}},
        {"instrument": "SZ000001", "ex_date": "2025-01-01", "operation": "replace", "values": {},
         "evidence": {"path": "not-queried-notice.json", "sha256": "0" * 64, "url": "https://issuer.example/unrelated"}}]}
    provider.overrides_path = case["tmp"] / "overrides.json"
    save(provider.overrides_path, document)
    result = provider.fetch_close("2026-10-19", ["SH600000"], case["clock"])
    adjusted = result["corporate_actions"][0]
    assert adjusted["cash_per_share"] == "0.20" and adjusted["reference_cash_per_share"] == "0.19"
    assert adjusted["reference_evidence_sha256"] == proof["sha256"]
    assert {raw["sha256"], proof["sha256"]}.issubset({row["sha256"] for row in result["evidence"]})
