"""After-cost reports use committed receipts; no source IO or actual account."""
from datetime import datetime
import hashlib
import json

import pytest

from stopat30m import ashare_forward
from stopat30m import ashare_paper_report as report


@pytest.fixture
def case(tmp_path, monkeypatch):
    root = tmp_path / "isolated-paper"
    root.mkdir()
    marker = root / "state.json"
    marker.write_text('{"fixture_marker":"never rewrite"}')
    manifest = {"created_at": "2026-10-19T18:00:00+08:00", "initial_capital_per_strategy": "100000.00",
        "strategies": ["model", "momentum"], "first_possible_execution": "2026-10-20"}
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


def receipt(root, state, session, *, failed=False, commission="5.00", transfer="0.20", stamp="0.00", fee="5.20"):
    trade = {"session": session, "status": "simulated_fill", "live_order": False,
        "commission": commission, "transfer_fee": transfer, "stamp_tax": stamp, "total_fee": fee, "slippage_cost": "2.00"}
    value = {"phase": "open", "session": session, "status": "failed" if failed else "completed",
        "accounts": {name: {"status": "completed", "trades": [trade]} for name in ("model", "momentum")}}
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
