"""After-cost reports use committed receipts; no source IO or actual account."""
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest

from stopat30m import ashare_forward
from stopat30m import ashare_paper_report as report


@pytest.fixture
def case(tmp_path, monkeypatch):
    root = tmp_path / "isolated-paper"
    root.mkdir()
    marker = root / "state.json"
    marker.write_text('{"fixture_marker":"never rewrite"}')
    manifest = {"kind": "isolated_ashare_integer_forward_paper",
        "created_at": "2026-10-19T18:00:00+08:00", "initial_capital_per_strategy": "100000.00",
        "execution_policy": {"minimum_commission": "5.00"},
        "strategies": ["model", "momentum"], "first_possible_execution": "2026-10-20"}
    (root / "manifest.json").write_text(json.dumps(manifest))
    state = {"receipts": [], "completed": {}, "valuations": [], "signals": {},
        "last_success": None, "last_failure": None, "observation_gaps": {}}
    status = {"path": str(root), "manifest": manifest, "state": state, "model_sha256": "a" * 64}
    monkeypatch.setattr(ashare_forward, "ashare_forward_status", lambda paper_dir: status)
    monkeypatch.setattr(report, "_now", lambda: datetime.fromisoformat("2026-10-23T08:00:00+08:00"))
    return root, state


def nav(state, session, lower, *, complete=True, estimate=None, upper=None, clock="17:15:00"):
    state["valuations"].append({"session": session, "observed_at": session + "T" + clock + "+08:00",
        "accounts": {name: {"equity_lower_bound": str(lower),
            "equity_with_claim_estimate": str(estimate if estimate is not None else lower),
            "equity_upper_bound": str(upper if upper is not None else lower),
            "valuation_complete": complete} for name in ("model", "momentum")}})


def receipt(root, state, session, *, failed=False, commission="5.00", transfer="0.20", stamp="0.00", fee="5.20",
            notional=None, orders=None):
    trade = {"session": session, "status": "simulated_fill", "live_order": False,
        "commission": commission, "transfer_fee": transfer, "stamp_tax": stamp, "total_fee": fee, "slippage_cost": "2.00"}
    if notional is not None:
        trade["notional"] = notional
    value = {"phase": "open", "session": session, "status": "failed" if failed else "completed",
        "accounts": {name: {"status": "completed", "trades": [trade]} for name in ("model", "momentum")}}
    if orders is not None:
        for account in value["accounts"].values():
            account["orders"] = orders
    file = root / (session + ("_failed.json" if failed else "_completed.json"))
    file.write_text(json.dumps(value))
    sha = hashlib.sha256(file.read_bytes()).hexdigest()
    state["receipts"].append({"path": str(file), "sha256": sha})
    if not failed:
        state["completed"][session + "_open"] = {**value, "receipt_path": str(file), "receipt_sha256": sha}
    return file


def test_drawdown_starts_at_initial_cash_and_fees_are_already_in_nav(case):
    root, state = case
    receipt(root, state, "2026-10-20")
    nav(state, "2026-10-20", 90000)
    nav(state, "2026-10-21", 95000)
    value = report.ashare_paper_summary(root)["accounts"]["model"]
    assert value["max_drawdown_lower"] == pytest.approx(-.10)
    assert value["net_pnl_lower"] == "-5000.00" and value["net_return_lower"] == pytest.approx(-.05)
    assert value["fees"] == {"commission": "5.00", "transfer_fee": "0.20", "stamp_tax": "0.00", "total_fee": "5.20", "slippage_cost": "2.00"}
    assert value["trade_count"] == 1 and value["annualized_return"] is None
    assert value["performance_status"] == "observed_net_loss"


def test_final_same_session_close_replaces_partial_for_curve_counts_and_drawdown(case, tmp_path):
    root, state = case
    receipt(root, state, "2026-10-20")
    nav(state, "2026-10-20", 60000, complete=False, estimate=75000, upper=80000, clock="16:45:00")
    nav(state, "2026-10-20", 99000, clock="17:15:00")
    nav(state, "2026-10-21", 98000, complete=False, estimate=98500, upper=99000)
    # An older duplicate arriving last cannot erase the later committed close.
    nav(state, "2026-10-20", 50000, complete=False, clock="16:40:00")
    value = report.ashare_paper_summary(root)["accounts"]["model"]
    assert value["valuation_days"] == 2 and value["incomplete_valuation_days"] == 1
    assert value["max_drawdown_lower"] == pytest.approx(-.02)
    assert value["last_nav"]["nav_estimate"] == "98500.00" and value["last_nav"]["nav_upper"] == "99000.00"
    assert value["performance_status"] == "partial_valuation"
    before = (root / "state.json").read_bytes()
    files = report.export_ashare_paper_report(root, tmp_path / "export")
    assert (root / "state.json").read_bytes() == before
    curve = __import__("pathlib").Path(files["curves_path"]).read_text()
    assert len(curve.splitlines()) == 7  # header + 2 * (initial baseline + 2 days).
    assert "60000" not in curve and "50000" not in curve


def test_failed_attempts_replayed_history_and_duplicate_refs_do_not_repeat_fees(case):
    root, state = case
    receipt(root, state, "2026-10-20", failed=True)
    receipt(root, state, "2026-10-20")
    state["receipts"].append(state["receipts"][-1].copy())  # Reading an existing receipt again is not another fill.
    receipt(root, state, "2026-10-21", stamp="5.00", fee="10.20")
    nav(state, "2026-10-21", 99984.60)
    value = report.ashare_paper_summary(root)["accounts"]["model"]
    assert value["trade_count"] == 2 and value["execution_sessions_with_fills"] == 2
    assert value["fees"]["total_fee"] == "15.40" and value["fees"]["slippage_cost"] == "4.00"
    assert value["net_pnl_lower"] == "-15.40"  # Fees and slippage must not be subtracted again.


def test_new_fills_without_new_close_have_unknown_current_return_and_no_fills_are_not_profit(case):
    root, state = case
    empty = report.ashare_paper_summary(root)["accounts"]["model"]
    assert empty["performance_status"] == "no_fills" and empty["net_pnl_lower"] == "0.00"
    receipt(root, state, "2026-10-20")
    waiting = report.ashare_paper_summary(root)["accounts"]["model"]
    assert waiting["performance_status"] == "awaiting_close_after_new_fills"
    assert waiting["net_pnl_lower"] is None and waiting["fills_after_last_nav"] is True
    nav(state, "2026-10-20", 99994.80)
    receipt(root, state, "2026-10-22")
    waiting = report.ashare_paper_summary(root)["accounts"]["model"]
    assert waiting["last_nav"]["session"] == "2026-10-20" and waiting["fills_after_last_nav"]


def test_receipt_tampering_and_inconsistent_fee_components_are_refused(case):
    root, state = case
    file = receipt(root, state, "2026-10-20")
    file.write_bytes(file.read_bytes() + b" ")
    with pytest.raises(ValueError, match="actual committed receipt"):
        report.ashare_paper_summary(root)
    state["receipts"].clear(); state["completed"].clear()
    receipt(root, state, "2026-10-20", fee="99.00")
    with pytest.raises(ValueError, match="fee components"):
        report.ashare_paper_summary(root)


def test_permanent_observation_gap_is_visible_and_blocks_next_execution(case):
    root, state = case
    state["signals"]["2026-10-22"] = {"signal_date": "2026-10-22", "execution_session": "2026-10-23",
        "created_at": "2026-10-22T17:15:00+08:00", "rebalance": True, "allowed": {"model": True, "momentum": True}}
    assert report.ashare_paper_summary(root)["next_possible_execution_session"] == "2026-10-23"
    state["observation_gaps"]["2026-10-21"] = {"reason": "missing_whole_session_close_observation"}
    result = report.ashare_paper_summary(root)
    assert result["observation_gaps"] and result["new_orders_frozen_by_observation_gap"]
    assert result["next_possible_execution_session"] is None
    with pytest.raises(ValueError, match="outside the paper"):
        report.export_ashare_paper_report(root, root / "reports")


def test_status_cli_defaults_to_summary_and_raw_is_explicit(case, monkeypatch):
    from click.testing import CliRunner
    from stopat30m.research_cli import research_group
    root, _ = case
    summary = CliRunner().invoke(research_group, ["ashare-forward-status", "--paper-dir", str(root)])
    assert summary.exit_code == 0 and json.loads(summary.output)["kind"] == "isolated_ashare_paper_summary"
    raw = CliRunner().invoke(research_group, ["ashare-forward-status", "--paper-dir", str(root), "--raw"])
    assert raw.exit_code == 0 and "state" in json.loads(raw.output)


def monitoring_case(root, state, monkeypatch, *, start="2026-10-19", end="2026-10-30", closed=(), created=None):
    """Explicit synthetic calendar; production never infers exchange weekdays."""
    from stopat30m.data.research_calendar import Calendar, natural_days
    sessions = [day for day in natural_days(start, end)
        if datetime.fromisoformat(day).weekday() < 5 and day not in closed]
    calendar = Calendar.from_sessions(sessions, start, end, source="synthetic_certified_calendar",
        source_url="https://example.invalid/test-calendar", fetched_at=start+"T00:00:00+08:00")
    calendar.save(root / "calendar.json")
    manifest = json.loads((root / "manifest.json").read_text())
    manifest["calendar_sha256"] = hashlib.sha256((root / "calendar.json").read_bytes()).hexdigest()
    if created:
        manifest["created_at"] = created
    (root / "manifest.json").write_text(json.dumps(manifest))
    status = {"path": str(root), "manifest": manifest, "state": state, "model_sha256": "a" * 64}
    monkeypatch.setattr(ashare_forward, "ashare_forward_status", lambda paper_dir: status)
    return sessions, status


def test_low_turnover_manifest_dispatches_without_loading_legacy_paper(case, monkeypatch):
    root, state = case
    manifest = json.loads((root / "manifest.json").read_text())
    manifest.update(kind="isolated_ashare_low_turnover_forward", strategies=["model"])
    (root / "manifest.json").write_text(json.dumps(manifest))
    state["accounts"] = {"model": {"entitlements": {}}}
    status = dict(path=str(root), manifest=manifest, state=state, model_sha256="b" * 64)
    module = ModuleType("stopat30m.ashare_low_turnover_forward")
    module.low_turnover_status = lambda paper_dir: status
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(ashare_forward, "ashare_forward_status", lambda *a: pytest.fail("legacy loader called"))
    value = report.ashare_paper_summary(root)
    assert set(value["accounts"]) == set(value["daily_monitoring"]["accounts"]) == {"model"}
    assert value["model_sha256"] == "b" * 64
    assert value["daily_summary_rows"] == []
    assert value["daily_monitoring"]["accounts"]["model"]["dividend_tax_recognized_cny"] == "0.00"
    assert value["daily_monitoring"]["accounts"]["model"]["performance_qualification_claimed"] is False


def test_unknown_paper_kind_is_not_reinterpreted_as_legacy(case):
    root, _ = case
    manifest = json.loads((root / "manifest.json").read_text()); manifest["kind"] = "unverified_paper"
    (root / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Unsupported"):
        report.ashare_paper_summary(root)


def test_new_conservative_drawdown_uses_upper_peak_without_changing_legacy_semantics(case):
    root, state = case
    receipt(root, state, "2026-10-20")
    nav(state, "2026-10-20", 100000, complete=False, estimate=110000, upper=120000)
    nav(state, "2026-10-21", 90000)
    value = report.ashare_paper_summary(root)
    assert value["accounts"]["model"]["max_drawdown_lower"] == pytest.approx(-.1)
    account = value["daily_monitoring"]["accounts"]["model"]
    assert account["max_drawdown_conservative"] == pytest.approx(-.25)
    assert account["latest"]["conservative_drawdown"] == pytest.approx(-.25)
    assert "partial_valuation_history" in account["diagnostic_reasons"]
    assert value["accounts"]["model"]["net_pnl_lower"] == "-10000.00"


def test_daily_costs_actual_notional_minimum_commissions_and_sidelss_rejections(case):
    root, state = case
    receipt(root, state, "2026-10-20", failed=True, notional="999999")
    receipt(root, state, "2026-10-20", notional="10000", orders=[{"status": "rejected"},
        {"status": "rejected", "side": "SELL"}, {"status": "simulated_fill"}])
    state["receipts"].append(state["receipts"][-1].copy())
    nav(state, "2026-10-20", "99992.80")
    value = report.ashare_paper_summary(root)
    row = value["daily_monitoring"]["accounts"]["model"]["latest"]
    assert row["trade_count"] == 1 and row["rejected_order_count"] == 2
    assert row["minimum_commission_fill_count"] == 1
    assert row["notional_cny"] == "10000.00"
    assert row["one_way_turnover_previous_close_nav"] == pytest.approx(.05)
    assert row["total_fee_cny"] == "5.20" and row["slippage_cost_cny"] == "2.00"
    assert row["fees_plus_slippage_cny"] == "7.20"
    assert value["accounts"]["model"]["net_pnl_lower"] == "-7.20"
    assert row["dividend_tax_recognized_cny"] is None  # Legacy missing account evidence is not zero tax.


def test_legacy_missing_notional_orders_or_calendar_are_unknown_not_guessed(case):
    root, state = case
    receipt(root, state, "2026-10-20")
    nav(state, "2026-10-20", "99994.80")
    value = report.ashare_paper_summary(root)
    account = value["daily_monitoring"]["accounts"]["model"]
    assert account["latest"]["notional_cny"] is None
    assert account["latest"]["rejected_order_count"] is None
    assert account["latest"]["daily_net_return"] is None
    assert account["stability"]["20"]["net_return_lower"] is None
    assert account["stability"]["20"]["status"] == "verified_calendar_unavailable"


def test_tax_entitlement_recognized_once_at_ex_date_not_again_on_settlement(case):
    root, state = case
    event = dict(kind="cash_dividend", ex_date="2026-10-20", pay_date="2026-10-21", status="settled",
        gross_cash="100.00", tax="20.00", net_cash="80.00")
    state["accounts"] = {name: {"entitlements": {"source-event:cash": event.copy()}} for name in ("model", "momentum")}
    receipt(root, state, "2026-10-20"); receipt(root, state, "2026-10-21")
    nav(state, "2026-10-20", "100080"); nav(state, "2026-10-21", "100080")
    value = report.ashare_paper_summary(root)
    rows = [row for row in value["daily_summary_rows"] if row["strategy"] == "model"]
    assert [row["dividend_tax_recognized_cny"] for row in rows] == ["20.00", "0.00"]
    assert value["daily_monitoring"]["accounts"]["model"]["dividend_tax_recognized_cny"] == "20.00"
    assert value["accounts"]["model"]["net_pnl_lower"] == "80.00"  # Never charge the tax again.
    event.pop("tax")
    state["accounts"]["model"]["entitlements"]["source-event:cash"] = event
    assert report.ashare_paper_summary(root)["daily_monitoring"]["accounts"]["model"]["dividend_tax_recognized_cny"] is None


def test_twenty_sixty_session_stability_uses_real_calendar_and_insufficient_samples_are_none(case, monkeypatch):
    root, state = case
    sessions, _ = monitoring_case(root, state, monkeypatch, start="2026-01-01", end="2026-06-30",
        closed=("2026-01-19",), created="2026-01-04T18:00:00+08:00")
    sessions = [day for day in sessions if day >= "2026-01-05"][:65]
    receipt(root, state, sessions[0], notional="10000", orders=[])
    monkeypatch.setattr(report, "_now", lambda: datetime.fromisoformat(sessions[-1]+"T17:15:00+08:00"))
    for number, day in enumerate(sessions):
        lower = (Decimal("100000") * Decimal("1.001") ** (number+1)).quantize(Decimal(".01"))
        nav(state, day, lower)
        if number == 0:
            first = report.ashare_paper_summary(root)["daily_monitoring"]["accounts"]["model"]
            assert first["stability"]["20"]["net_return_lower"] is None
            assert first["stability"]["60"]["net_return_lower"] is None
    before = (root / "state.json").read_bytes()
    value = report.ashare_paper_summary(root)["daily_monitoring"]["accounts"]["model"]
    assert value["status"] == "observed_after_cost_monitoring"
    assert value["stability"]["20"]["observed_complete_sessions"] == 20
    assert value["stability"]["60"]["observed_complete_sessions"] == 60
    assert value["stability"]["20"]["positive_session_fraction"] == 1
    assert value["stability"]["60"]["net_return_lower"] == pytest.approx(float(Decimal(state["valuations"][-1]["accounts"]["model"]["equity_lower_bound"])
        / Decimal(state["valuations"][4]["accounts"]["model"]["equity_lower_bound"]) - 1))
    assert value["annualized_return"] is None and value["performance_qualification_claimed"] is False
    assert (root / "state.json").read_bytes() == before
    # A genuine source-session hole is not treated like the explicit holiday.
    missing = sessions[-5]
    state["valuations"] = [row for row in state["valuations"] if row["session"] != missing]
    state["observation_gaps"][missing] = {"reason": "missing_whole_session_close_observation"}
    value = report.ashare_paper_summary(root)["daily_monitoring"]["accounts"]["model"]
    assert value["stability"]["20"]["net_return_lower"] is None
    assert value["stability"]["60"]["net_return_lower"] is None
    assert "permanent_observation_gap" in value["diagnostic_reasons"]


def test_stale_close_and_new_fills_are_diagnostic_without_inventing_today_nav(case, monkeypatch):
    root, state = case
    monitoring_case(root, state, monkeypatch, created="2026-10-18T18:00:00+08:00")
    receipt(root, state, "2026-10-19", notional="10000", orders=[])
    nav(state, "2026-10-19", "99992.80")
    receipt(root, state, "2026-10-22", notional="2000", orders=[])
    value = report.ashare_paper_summary(root)["daily_monitoring"]["accounts"]["model"]
    assert value["latest_session"] == "2026-10-22" and value["latest_close_session"] == "2026-10-19"
    assert value["latest"]["nav_lower"] is None and value["latest"]["daily_net_return"] is None
    assert {"latest_close_is_stale", "new_activity_awaiting_saved_close"}.issubset(value["diagnostic_reasons"])
    assert not state["observation_gaps"]  # Reporting never changes runner state.


def test_partial_interval_and_final_close_replacement_do_not_create_daily_returns(case, monkeypatch):
    root, state = case
    monitoring_case(root, state, monkeypatch, created="2026-10-18T18:00:00+08:00")
    receipt(root, state, "2026-10-19", notional="10000", orders=[])
    nav(state, "2026-10-19", "90000", complete=False, estimate=95000, upper=100000)
    nav(state, "2026-10-20", "98000")
    value = report.ashare_paper_summary(root)
    assert all(row["daily_net_return"] is None for row in value["daily_summary_rows"])
    nav(state, "2026-10-19", "99000", clock="17:20:00")
    rows = [row for row in report.ashare_paper_summary(root)["daily_summary_rows"] if row["strategy"] == "model"]
    assert len(rows) == 2 and all(row["daily_net_return"] is not None for row in rows)
    assert rows[-1]["daily_net_return"] == pytest.approx(98000/99000-1)


def test_daily_csv_export_is_additive_and_calendar_tampering_is_rejected(case, monkeypatch, tmp_path):
    import csv
    root, state = case
    monitoring_case(root, state, monkeypatch, created="2026-10-18T18:00:00+08:00")
    receipt(root, state, "2026-10-19", notional="10000", orders=[])
    nav(state, "2026-10-19", "99992.80")
    before = {path: path.read_bytes() for path in root.iterdir() if path.is_file()}
    files = report.export_ashare_paper_report(root, tmp_path / "daily-export")
    assert {"summary_path", "curves_path", "daily_summary_path"} == set(files)
    with Path(files["daily_summary_path"]).open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2 and set(rows[0]) == set(report.DAILY_FIELDS)
    assert rows[0]["fees_plus_slippage_cny"] == "7.20"
    assert all(path.read_bytes() == value for path, value in before.items())
    (root / "calendar.json").write_bytes((root / "calendar.json").read_bytes()+b" ")
    with pytest.raises(ValueError, match="calendar differs"):
        report.ashare_paper_summary(root)
