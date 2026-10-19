"""Attribution must conserve posted cash and preserve predictive timing."""
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from stopat30m.ashare_diagnostics import (
    classify_trades, ledger_cost_attribution, predictive_daily_statistics,
    signal_stability, summarize_predictive_statistics, verify_artifact,
)


def test_artifact_inventory_tamper_is_rejected_before_reading(tmp_path):
    file = tmp_path / "trades.csv"
    file.write_bytes(b"original")
    (tmp_path / "manifest.json").write_text(json.dumps({"files": {"trades.csv": hashlib.sha256(file.read_bytes()).hexdigest()}}))
    assert verify_artifact(tmp_path)[1]["verified_files"] == 1
    file.write_bytes(b"replacement")
    with pytest.raises(ValueError, match="checksum"):
        verify_artifact(tmp_path)


def test_fixed_ledger_bridge_does_not_double_charge_slippage():
    trades = pd.DataFrame([dict(session="2025-01-02", instrument="SH600000", side="BUY", quantity="100", reference_price="10.00",
        execution_price="10.01", notional="1001.00", commission="5.00", transfer_fee="0.01", stamp_tax="0.00",
        total_fee="5.01", slippage_cost="1.00")])
    curves = pd.DataFrame([dict(date="2025-01-02", equity_cny="99993.99", cash_cny="98993.99",
        commission_cny="5.00", transfer_fee_cny="0.01", stamp_tax_cny="0.00", slippage_cny="1.00")])
    signals = pd.DataFrame([dict(session="2025-01-02", instrument="SH600000", selected="True")])
    positions = pd.DataFrame([dict(session="2025-01-02", instrument="SH600000", quantity="100")])
    orders = pd.DataFrame([dict(status="simulated_fill", reason="", requested_quantity="100", instrument="SH600000")])
    policy = dict(initial_cash="100000", commission_rate="0.0003", minimum_commission="5", slippage_bps="2")
    result, _ = ledger_cost_attribution(trades, curves, signals, orders, positions, policy)
    assert result["fixed_ledger_pnl_bridge_cny"]["reference_fill_before_direct_fees"] == "0.00"
    assert result["costs"]["minimum_commission_excess_cny"] == "4.70"
    assert result["costs"]["adverse_tick_rounding_excess_cny"] == "0.80"
    assert result["fixed_ledger_addback_is_not_a_counterfactual_rerun"] is True
    curves.loc[0, "slippage_cny"] = "2.00"
    with pytest.raises(ValueError, match="reconciliation"):
        ledger_cost_attribution(trades, curves, signals, orders, positions, policy)


def test_a_retained_unfilled_target_is_not_repeated_resize():
    calendar = ["2025-01-02", "2025-01-03", "2025-01-06"]
    signals = pd.DataFrame([dict(session=day, instrument="SH600000", selected=True) for day in (calendar[0], calendar[2])])
    trades = pd.DataFrame([dict(session=calendar[2], instrument="SH600000", side="BUY")])
    empty_positions = pd.DataFrame(columns=["session", "instrument", "quantity"])
    result = classify_trades(trades, signals, empty_positions, calendar)
    assert result.iloc[0].trade_role == "retained_target_first_fill"
    positions = pd.DataFrame([dict(session=calendar[1], instrument="SH600000", quantity=100)])
    assert classify_trades(trades, signals, positions, calendar).iloc[0].trade_role == "retained_holding_resize"
    stability, _ = signal_stability(signals)
    assert stability["mean_retained_fraction"] == 1


def test_forward_labels_enter_next_open_and_stop_at_bound_end():
    calendar = pd.bdate_range("2025-01-02", periods=8).strftime("%Y-%m-%d").tolist()
    symbols = [f"SH{600000 + i}" for i in range(25)]
    scores = pd.DataFrame([dict(datetime=calendar[0], instrument=instrument, score=i) for i, instrument in enumerate(symbols)])
    prices = pd.DataFrame(10., index=calendar, columns=symbols)
    prices.iloc[0] = 1  # Same-day open is deliberately unrelated to labels.
    prices.iloc[1] = 10
    prices.iloc[6] = 10 + np.arange(25)
    result = predictive_daily_statistics(scores, prices, calendar, end=calendar[6], horizons=(5, 6))
    assert len(result) == 1  # Six-session exit would exceed cutoff.
    assert result.iloc[0].entry_date == calendar[1] and result.iloc[0].exit_date == calendar[6]
    assert result.iloc[0].rank_ic == pytest.approx(1)
    assert result.iloc[0].universe_gross_forward_return == pytest.approx(1.2)
    assert result.iloc[0].top20_gross_forward_return == pytest.approx(1.45)
    summary = summarize_predictive_statistics(result)
    assert summary["2025"]["5"]["statistics"]["rank_ic"]["hac_lag_sessions"] == 5


def test_duplicate_scores_cannot_change_quintile_weights():
    scores = pd.DataFrame([dict(datetime="2025-01-02", instrument="SH600000", score=1)] * 2)
    with pytest.raises(ValueError, match="Duplicate predictions"):
        predictive_daily_statistics(scores, pd.DataFrame(), ["2025-01-02"], end="2025-01-02")
