"""Execution engineering tests; no historical return is used to choose a rule."""
from dataclasses import replace
from decimal import Decimal as D

import pandas as pd
import pytest

from stopat30m.backtest import ashare_execution as ex
from stopat30m.ashare_optimization_execution import (
    RankBufferPolicy, build_rank_buffer_plan, execute_rank_buffer_plan,
)

A, B, C, STAR = "SH600000", "SZ000001", "SH600009", "SH688001"
SIGNAL, DAY = "2024-12-31", "2025-01-02"
CAL = pd.bdate_range("2024-11-01", "2025-02-01").strftime("%Y-%m-%d").tolist()  # Synthetic fixture only.


def quote(price="10", **kwargs):
    return ex.OpeningQuote(DAY, D(price), D("10"), "trading", "synthetic observed raw opening", **kwargs)


def metadata(inst):
    return ex.SecurityInfo(inst, "star" if inst == STAR else "sse_main", "2000-01-03", False,
        "2024-12-01T23:59:59+08:00", "synthetic prior-known identity")


def plan(state, ranked=(A, B), *, prices=None, policy=None, equity="10000", **kwargs):
    return build_rank_buffer_plan(state, signal_date=SIGNAL, ranked_instruments=ranked,
        raw_signal_prices=prices or {inst: "10" for inst in ranked},
        boards={inst: "star" if inst == STAR else "sse_main" for inst in ranked},
        signal_equity=equity, policy=policy or RankBufferPolicy(top_k=2, keep_rank=4), **kwargs)


def execute(state, frozen, *, quotes=None, policy=None):
    symbols = set(state.positions) | set(frozen.selected)
    return execute_rank_buffer_plan(state, plan=frozen, session=DAY,
        quotes=quotes if quotes is not None else {inst: quote() for inst in symbols},
        metadata={inst: metadata(inst) for inst in symbols}, calendar=CAL, policy=policy)


def test_rank_buffer_keeps_incumbent_below_entry_cutoff_until_exit_rank():
    state = ex.PortfolioState(D("5000"), {A: ex.Position(500, last_mark=D(10), last_mark_date=SIGNAL)})
    frozen = plan(state, (B, C, A, STAR))
    assert frozen.retained == (A,) and frozen.selected == (A, B)
    moved = plan(state, (B, C, STAR, "SH600010", A),
        prices={inst: "10" for inst in (B, C, STAR, "SH600010", A)})
    assert moved.retained == () and moved.exits == (A,) and moved.selected == (B, C)


def test_signal_affordability_filter_uses_board_minimum_tick_slippage_and_fee():
    state = ex.PortfolioState(D("10000"))
    high_price = {STAR: "25", A: "10", B: "10"}
    unfiltered = plan(state, (STAR, A, B), prices=high_price)
    assert unfiltered.selected == (STAR, A) and unfiltered.entries[0][1] == 0
    filtered = plan(state, (STAR, A, B), prices=high_price,
        policy=RankBufferPolicy(top_k=2, keep_rank=4, require_signal_affordability=True))
    assert filtered.selected == (A, B)
    assert filtered.filtered[0][0] == STAR
    # 200 * 25 = 5,000 consumes the entire slot before its 5 yuan minimum fee.
    assert len(filtered.filtered) == 1


def test_star_and_odd_incumbent_quantities_never_receive_one_share_reweight():
    state = ex.PortfolioState(D("5000"), {
        STAR: ex.Position(201, last_mark=D("11.03"), last_mark_date=SIGNAL),
        A: ex.Position(399, last_mark=D("10.03"), last_mark_date=SIGNAL)})
    before = state.to_dict()
    frozen = plan(state, (STAR, A))
    result = execute(state, frozen, quotes={STAR: quote("10.03"), A: quote("9.97")})
    assert not result.trades and not result.orders
    assert result.state.positions[STAR].quantity == 201
    assert result.state.positions[A].quantity == 399
    assert result.state.cash == state.cash and state.to_dict() == before


def test_exit_and_entry_cash_fees_and_closing_nav_use_original_engine():
    state = ex.PortfolioState(D("5000"), {C: ex.Position(500, last_mark=D(10), last_mark_date=SIGNAL)})
    frozen = plan(state, (A, B, C), policy=RankBufferPolicy(top_k=2, keep_rank=2))
    result = execute(state, frozen)
    assert result.trades[0]["side"] == "SELL" and result.trades[0]["instrument"] == C
    assert {row["instrument"] for row in result.trades if row["side"] == "BUY"} == {A, B}
    assert all(D(row["commission"]) >= 5 for row in result.trades)
    assert result.state.cash == state.cash + sum(D(row["cash_change"]) for row in result.trades)
    assert result.opening_equity == D("10000")
    valuation = ex.mark_to_close(result.state, session=DAY, prices={A: 10, B: 10}, calendar=CAL)
    direct = result.state.cash + sum(position.quantity * D(10) for position in result.state.positions.values())
    assert valuation.equity_lower_bound == direct


def test_retained_receivables_and_fractional_claims_are_not_spendable_sleeve_cash():
    state = ex.PortfolioState(D("1000"), {A: ex.Position(400, last_mark=D(10), last_mark_date=SIGNAL)},
        entitlements={"cash": {"instrument": A, "kind": "cash_dividend", "status": "pending", "net_cash": "100"},
                      "bonus": {"instrument": A, "kind": "bonus_shares", "status": "pending", "quantity": "100", "last_mark": "10"}},
        claims={"tail": {"instrument": A, "quantity": ".2", "last_mark": "10", "maximum_additional_integer_shares": 1}})
    frozen = plan(state, (A, B), equity="6100")
    result = execute(state, frozen)
    assert result.state.positions[A].quantity == 400
    assert result.state.entitlements == state.entitlements and result.state.claims == state.claims
    assert result.audit["sleeve_opening_equity_cny"] == "1000.00"
    assert result.opening_equity == D("6100")
    assert sum(D(row["notional"])+D(row["total_fee"]) for row in result.trades) <= 1000


def test_pending_bonus_without_delivered_position_retains_a_slot_and_rights():
    state = ex.PortfolioState(D("9000"), entitlements={"bonus": {
        "instrument": A, "kind": "bonus_shares", "status": "pending", "quantity": "100", "last_mark": "10"}})
    frozen = plan(state, (B, A))
    assert frozen.retained == (A,) and {row[0] for row in frozen.entries} == {B}
    result = execute(state, frozen)
    assert A not in {row["instrument"] for row in result.trades}
    assert result.state.entitlements == state.entitlements


def test_opening_gap_only_scales_new_quantities_down_and_never_replaces_stocks():
    state = ex.PortfolioState(D("100000"))
    frozen = plan(state, equity="100000")
    quantities = {inst: quantity for inst, quantity, _, _ in frozen.entries}
    # Use a valid +9% observation; the extra fee/tick budget may reduce the lot.
    result = execute(state, frozen, quotes={A: quote("10.90"), B: quote("10.90")})
    assert D(result.audit["opening_budget_scale"]) < 1
    assert result.audit["opening_replacements"] == []
    assert all(row["quantity"] <= quantities[row["instrument"]] for row in result.trades)
    assert result.state.cash >= 0
    cheap = execute(state, frozen, quotes={A: quote("9.10"), B: quote("9.10")})
    assert all(row["quantity"] <= quantities[row["instrument"]] for row in cheap.trades)


def test_upper_limit_and_missing_quote_never_select_a_lower_rank_replacement():
    state = ex.PortfolioState(D("10000"))
    frozen = plan(state, (A, B, C))
    result = execute(state, frozen, quotes={A: quote("11")})
    assert result.audit["opening_replacements"] == []
    assert not result.trades and C not in frozen.selected
    assert any("upper-limit" in row["reason"] for row in result.orders)
    assert any("Missing quote" in row["reason"] for row in result.orders)


def test_t1_locked_exit_cannot_fund_a_replacement_and_retained_lock_is_preserved():
    state = ex.PortfolioState(D(0), {C: ex.Position(1000, bought_today=1000, bought_date=DAY,
        last_mark=D(10), last_mark_date=DAY)}, session=DAY)
    frozen = plan(state, (A, B, C), policy=RankBufferPolicy(top_k=2, keep_rank=2))
    result = execute(state, frozen)
    assert not result.trades and result.state.positions[C].quantity == 1000 and result.state.cash == 0
    kept_plan = plan(state, (C, A))
    kept = execute(state, kept_plan)
    assert kept.state.positions[C].bought_today == 1000
    assert not any(row["instrument"] == C for row in kept.orders)


def test_unknown_held_price_gap_is_validated_even_for_retained_sleeve():
    state = ex.PortfolioState(D("5000"), {A: ex.Position(500, last_mark=D(10), last_mark_date="2024-11-01")})
    frozen = plan(state)
    with pytest.raises(ValueError, match="Unknown held opening-price gap"):
        execute(state, frozen, quotes={B: quote()})


def test_new_day_advances_retained_t1_without_mutating_original_state():
    state = ex.PortfolioState(D("5000"), {A: ex.Position(500, bought_today=500, bought_date=SIGNAL,
        last_mark=D(10), last_mark_date=SIGNAL)}, session=SIGNAL)
    frozen = plan(state, (A, B))
    result = execute(state, frozen)
    assert result.state.positions[A].bought_today == 0
    assert state.positions[A].bought_today == 500


@pytest.mark.parametrize("cash", ["17234.56", "19031.09", "23456.78", "100000.00"])
def test_integer_share_instruction_survives_recurring_weight_division(cash):
    state = ex.PortfolioState(D(cash))
    frozen = plan(state, (STAR,), equity=cash, policy=RankBufferPolicy(top_k=1, keep_rank=1))
    wanted = frozen.entries[0][1]
    result = execute(state, frozen)
    assert len(result.trades) == 1 and result.trades[0]["quantity"] == wanted
    assert result.trades[0]["requested_quantity"] == wanted
    assert D(result.audit["weight_sum"]) <= 1


def test_an_unexpected_position_after_freezing_fails_before_execution():
    state = ex.PortfolioState(D("10000"))
    frozen = plan(state)
    state.positions[C] = ex.Position(100, last_mark=D(10), last_mark_date=SIGNAL)
    with pytest.raises(ValueError, match="exposure changed"):
        execute(state, frozen)


@pytest.mark.parametrize("value", [0, -1, "NaN", True])
def test_invalid_prior_equity_is_rejected(value):
    with pytest.raises(ValueError):
        plan(ex.PortfolioState.initial(), equity=value)


def test_prior_raw_prices_cannot_be_future_missing_or_adjusted_fractional_ticks():
    state = ex.PortfolioState.initial()
    with pytest.raises(ValueError, match="preceding raw close"):
        plan(state, prices={A: 10})
    with pytest.raises(ValueError, match="raw CNY ticks"):
        plan(state, prices={A: "10.123", B: 10})
    with pytest.raises(ValueError, match="unique"):
        plan(state, (A, A))


def test_plan_cannot_execute_on_signal_date_or_after_a_finalized_close():
    state = ex.PortfolioState.initial()
    frozen = plan(state)
    with pytest.raises(ValueError, match="preceding real"):
        execute_rank_buffer_plan(state, plan=replace(frozen, signal_date=DAY), session=DAY,
            quotes={}, metadata={}, calendar=CAL)
    finalized = ex.mark_to_close(state, session=DAY, prices={}, calendar=CAL).state
    with pytest.raises(ValueError, match="finalized closing"):
        execute(finalized, frozen)
