"""Synthetic accounting/rule boundaries, not historical strategy performance."""
from dataclasses import replace
from decimal import Decimal as D

import pandas as pd
import pytest

from stopat30m.backtest.ashare_execution import (
    ExecutionPolicy, OpeningQuote, PortfolioState, Position, SecurityInfo,
    apply_bonus_shares, apply_cash_dividend, apply_share_exchange,
    execute_target_weights, fees_for_order, mark_to_close, price_limits,
    register_dividend_entitlement, register_bonus_entitlement,
)

A, B = "SH600000", "SZ000001"
CAL = pd.bdate_range("2024-12-02", "2026-12-31").strftime("%Y-%m-%d").tolist()  # Explicit synthetic calendar.
DAY, NEXT = "2025-01-02", "2025-01-03"
SHA = "a" * 64


def info(inst=A, board="sse_main", **kwargs):
    return SecurityInfo(inst, board, "2000-01-03", False, "2024-12-01T23:59:59+08:00", "synthetic historical metadata", **kwargs)


def quote(day=DAY, price="10", preclose="10", **kwargs):
    return OpeningQuote(day, D(price) if price is not None else None, D(preclose), "trading", "synthetic raw source", **kwargs)


def execute(state=None, *, day=DAY, targets=None, quotes=None, metadata=None, policy=None, **kwargs):
    signal = CAL[CAL.index(day) - 1]
    return execute_target_weights(state or PortfolioState.initial(), session=day, signal_date=signal,
        targets=targets if targets is not None else {A: D(".5")},
        quotes=quotes if quotes is not None else {A: quote(day)},
        metadata=metadata if metadata is not None else {A: info()}, calendar=CAL, policy=policy, **kwargs)


def test_costs_are_rounded_per_order_and_every_cash_cent_reconciles():
    result = execute()
    trade = result.trades[0]
    assert trade["quantity"] == 5000 and trade["execution_price"] == "10.01"
    assert D(trade["commission"]) == D("15.02")
    assert D(trade["transfer_fee"]) == D(".50")
    assert D(trade["stamp_tax"]) == 0
    assert D(trade["cash_before"]) + D(trade["cash_change"]) == D(trade["cash_after"]) == result.state.cash
    assert D(trade["cash_change"]) == -(D(trade["notional"]) + D(trade["total_fee"]))
    assert result.state.positions[A].quantity == 5000
    assert result.state.positions[A].bought_today == 5000
    assert result.state.positions[A].last_mark == 10  # Market mark, not our adverse slip fill.


def test_minimum_commission_cash_boundary_and_buy_lot_rounding():
    policy = ExecutionPolicy(slippage_bps=0)
    exact = execute(PortfolioState(D("1005.01")), targets={A: 1}, policy=policy)
    assert exact.trades[0]["quantity"] == 100 and exact.state.cash == 0
    assert exact.trades[0]["commission"] == "5.00" and exact.trades[0]["transfer_fee"] == "0.01"
    short = execute(PortfolioState(D("1005.00")), targets={A: 1}, policy=policy)
    assert not short.trades and short.state.cash == D("1005.00")
    assert "Cash cannot cover" in short.orders[0]["reason"]
    large = fees_for_order(10000, "10", "SELL")
    assert large == {"notional": D("100000.00"), "commission": D("30.00"), "transfer_fee": D("1.00"), "stamp_tax": D("50.00"), "total_fee": D("81.00")}


def test_t1_is_not_reset_by_a_second_same_day_call_but_unlocks_next_session():
    bought = execute()
    same = execute(bought.state, targets={})
    assert not same.trades and same.state.positions[A].quantity == 5000
    assert "T+1" in same.orders[0]["reason"]
    sold = execute(same.state, day=NEXT, targets={})
    assert not sold.state.positions and sold.trades[0]["side"] == "SELL"
    assert sold.trades[0]["quantity"] == 5000
    assert D(sold.trades[0]["stamp_tax"]) > 0


@pytest.mark.parametrize("board,weight,expected", [("sse_main", ".0151", 100), ("chinext", ".0151", 100), ("star", ".0199", 0), ("star", ".0201", 201), ("bse", ".0101", 101)])
def test_board_lots_have_distinct_minima_and_increments(board, weight, expected):
    inst = {"star": "SH688001", "chinext": "SZ300001", "bse": "BJ920001"}.get(board, A)
    result = execute(targets={inst: D(weight)}, quotes={inst: quote()}, metadata={inst: info(inst, board)})
    assert sum(row["quantity"] for row in result.trades) == expected


@pytest.mark.parametrize("held,target,expected", [(299, ".001", 200), (299, 0, 299), (99, 0, 99), (300, ".001", 200)])
def test_main_board_odd_remainder_is_sold_whole_never_split(held, target, expected):
    # Small target notional produces varying reductions; an odd remainder can
    # be included only in its entirety, per SSE's explicit 299-share examples.
    state = PortfolioState(D("100000"), {A: Position(held, last_mark=D(10), last_mark_date="2024-12-31")})
    result = execute(state, targets={A: D(target)})
    quantity = result.trades[0]["quantity"] if result.trades else 0
    assert quantity % 100 in {0, held % 100}
    assert quantity == expected


@pytest.mark.parametrize("session,st,board,rate", [("2025-06-30", True, "sse_main", ".05"), ("2026-07-03", True, "szse_main", ".05"), ("2026-07-06", True, "sse_main", ".10"), ("2026-07-06", True, "szse_main", ".10"), (DAY, True, "star", ".20"), (DAY, True, "chinext", ".20"), (DAY, False, "bse", ".30")])
def test_limit_rules_use_effective_date_and_board_not_the_consultation_date(session, st, board, rate):
    rule = price_limits(replace(info(board=board), is_st=st), quote(session), session=session, calendar=CAL)
    assert rule["rate"] == D(rate)


def test_limit_rounding_is_decimal_half_up_and_low_price_has_at_least_one_tick():
    rule = price_limits(info(), quote(preclose="10.05"), session=DAY, calendar=CAL)
    assert rule["upper"] == D("11.06")  # 11.055 half up, never Python bankers round.
    penny = price_limits(info(), quote(price=".02", preclose=".02"), session=DAY, calendar=CAL)
    assert penny["upper"] == D(".03") and penny["lower"] == D(".01")


def test_adverse_limit_blocks_buy_or_sell_without_reading_the_days_high_low_or_close():
    up = execute(quotes={A: quote(price="11")})
    assert not up.trades and "upper-limit" in up.orders[0]["reason"]
    state = PortfolioState(D(100), {A: Position(100, last_mark=D(10), last_mark_date="2024-12-31")})
    down = execute(state, targets={}, quotes={A: quote(price="9")})
    assert not down.trades and down.state.positions[A].quantity == 100
    assert "lower-limit" in down.orders[0]["reason"]
    capped = execute(quotes={A: quote(price="10.99")})
    assert D(capped.trades[0]["execution_price"]) == 11


def test_ipo_first_five_real_sessions_are_identified_and_conservatively_not_traded():
    security = replace(info(), listing_date=DAY)
    for day in CAL[CAL.index(DAY):CAL.index(DAY) + 5]:
        rule = price_limits(security, quote(day), session=day, calendar=CAL)
        assert rule["no_limit_ipo"] and rule["upper"] is None
        result = execute(day=day, metadata={A: security})
        assert not result.trades and "IPO no-limit" in result.orders[0]["reason"]
    sixth = CAL[CAL.index(DAY) + 5]
    assert not price_limits(security, quote(sixth), session=sixth, calendar=CAL)["no_limit_ipo"]


@pytest.mark.parametrize("security", [replace(info(), is_st=None), replace(info(), known_at="2025-01-02T10:00:00+08:00"), replace(info(), board="unknown"), replace(info(), status_session=NEXT)])
def test_unknown_or_future_metadata_is_an_explicit_rejection_not_default_ten_percent(security):
    result = execute(metadata={A: security})
    assert not result.trades and result.orders[0]["status"] == "rejected"


def test_suspension_missing_price_and_stale_live_quotes_never_get_historical_fills():
    suspended = replace(quote(), open=None, tradestatus="suspended")
    assert not execute(quotes={A: suspended}).trades
    now = DAY + "T09:35:00+08:00"
    refused = execute(now=now)
    assert not refused.trades and "Historical" in refused.orders[0]["reason"]
    live = replace(quote(), execution_price=D("10.50"), execution_price_basis="live_observed_quote", asof=DAY + "T09:34:40+08:00")
    result = execute(quotes={A: live}, now=now)
    assert result.trades[0]["reference_price"] == "10.50" and result.trades[0]["execution_price"] == "10.51"
    stale = execute(quotes={A: replace(live, asof=DAY + "T09:30:00+08:00")}, now=now)
    assert not stale.trades and "stale" in stale.orders[0]["reason"]
    lunch = execute(quotes={A: replace(live, asof=DAY + "T12:00:00+08:00")}, now=DAY + "T12:00:30+08:00")
    assert not lunch.trades and "outside" in lunch.orders[0]["reason"]


def test_state_serialization_is_exact_and_input_state_is_never_mutated():
    original = PortfolioState.initial()
    before = original.to_dict()
    result = execute(original)
    assert original.to_dict() == before
    restored = PortfolioState.from_dict(result.state.to_dict())
    assert restored.to_dict() == result.state.to_dict()
    assert execute(restored, day=NEXT, targets={}).trades == execute(result.state, day=NEXT, targets={}).trades


def test_close_carry_is_valuation_only_and_unknown_long_gap_stops():
    bought = execute()
    marked = mark_to_close(bought.state, session=DAY, prices={A: D("10.10")}, calendar=CAL)
    missing = mark_to_close(marked.state, session=NEXT, prices={}, calendar=CAL)
    assert not missing.valuation_complete and missing.positions[0]["valuation_source"] == "carried_observed_mark"
    assert missing.state.positions[A].last_mark == D("10.10")
    late = CAL[CAL.index(DAY) + 21]
    with pytest.raises(ValueError, match="gap exceeds"):
        mark_to_close(marked.state, session=late, prices={}, calendar=CAL)
    known = mark_to_close(marked.state, session=late, prices={}, calendar=CAL, verified_suspended=[A])
    assert not known.valuation_complete and known.positions[0]["stale_sessions"] == 21


def test_verified_integer_merge_floors_shares_preserves_uncertain_remainder_and_is_idempotent():
    state = PortfolioState(D(100), {A: Position(101, last_mark=D(10), last_mark_date="2024-12-31")})
    args = dict(action_id="verified_merge", from_instrument=A, to_instrument=B, ratio=".62", session=DAY,
        tradable_from=DAY, confirmed_known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA,
        calendar=CAL, successor_price=D(16))
    changed = apply_share_exchange(state, **args)
    assert A not in changed.state.positions and changed.state.positions[B].quantity == 62
    assert changed.state.claims["verified_merge"]["quantity"] == "0.62"
    assert changed.state.cash == state.cash and changed.record["fee"] == 0
    assert apply_share_exchange(changed.state, **args).replayed
    with pytest.raises(ValueError, match="different content"):
        apply_share_exchange(changed.state, **{**args, "ratio": ".63"})
    marked = mark_to_close(changed.state, session=DAY, prices={B: D(16)}, calendar=CAL)
    assert marked.equity_lower_bound == D(1092)
    assert marked.equity_with_claim_estimate == D("1101.92")
    assert marked.equity_upper_bound == D(1108) and not marked.valuation_complete
    assert not marked.claims[0]["spendable"]


def test_dividends_follow_record_day_ownership_and_payment_date_with_separate_tax():
    state = PortfolioState(D(100), {A: Position(101, last_mark=D(10), last_mark_date=DAY)})
    state = mark_to_close(state, session=DAY, prices={A: D(10)}, calendar=CAL).state
    payment = "2025-01-06"
    state = register_dividend_entitlement(state, event_id="dividend", instrument=A, record_date=DAY,
        ex_date=NEXT, pay_date=payment, cash_per_share=".10", session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL).state
    state.positions = {}  # Sold after record date; payment still belongs to the old holder.
    receivable = mark_to_close(state, session=NEXT, prices={}, calendar=CAL)
    assert receivable.equity_lower_bound == D("108.08") and receivable.state.cash == 100
    args = dict(event_id="dividend", instrument=A, record_date=DAY, pay_date=payment, cash_per_share=".10",
        session=payment, known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL)
    paid = apply_cash_dividend(state, **args)
    assert paid.state.cash == D("108.08")
    assert paid.record["gross_cash"] == "10.10" and paid.record["tax"] == "2.02"
    assert paid.record["record_date_shares"] == 101
    assert apply_cash_dividend(paid.state, **args).state.cash == paid.state.cash
    with pytest.raises(ValueError, match="payment date"):
        apply_cash_dividend(state, **{**args, "session": NEXT})
    assert mark_to_close(paid.state, session=payment, prices={}, calendar=CAL).equity_lower_bound == D("108.08")
    with pytest.raises(ValueError, match="registered ex-date"):
        apply_cash_dividend(PortfolioState(D(100)), **args)


def test_bonus_shares_use_record_snapshot_and_only_unlock_on_confirmed_listing_date():
    state = PortfolioState(D(100), {A: Position(101, last_mark=D(10), last_mark_date=DAY)})
    state = mark_to_close(state, session=DAY, prices={A: 10}, calendar=CAL).state
    listing = "2025-01-06"
    state = register_bonus_entitlement(state, event_id="bonus", instrument=A, record_date=DAY,
        ex_date=NEXT, listing_date=listing, ratio=".15", session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL, price=D(9)).state
    args = dict(event_id="bonus", instrument=A, record_date=DAY, listing_date=listing, ratio=".15", session=listing,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL, price=D(9))
    with pytest.raises(ValueError, match="before.*listing"):
        apply_bonus_shares(state, **{**args, "session": NEXT})
    bonus = apply_bonus_shares(state, **args)
    assert bonus.state.positions[A].quantity == 116
    assert bonus.state.positions[A].bought_today == 0
    assert bonus.state.claims["bonus"]["quantity"] == "0.15"
    assert apply_bonus_shares(bonus.state, **args).replayed


def test_exdate_receivables_prevent_artificial_raw_price_losses_before_payment_or_listing():
    state = PortfolioState(D(100), {A: Position(100, last_mark=D(10), last_mark_date=DAY)})
    state = mark_to_close(state, session=DAY, prices={A: 10}, calendar=CAL).state
    # A one-for-one bonus halves the observed raw price. The pending 100 shares
    # must already count as an illiquid entitlement before their listing day.
    listing = "2025-01-06"
    registered = register_bonus_entitlement(state, event_id="oneforone", instrument=A, record_date=DAY,
        ex_date=NEXT, listing_date=listing, ratio=1, session=NEXT, known_at="2024-12-31T23:59:59+08:00",
        evidence_sha256=SHA, calendar=CAL, price=5)
    exmark = mark_to_close(registered.state, session=NEXT, prices={A: 5}, calendar=CAL)
    assert exmark.equity_lower_bound == 1100
    assert exmark.state.positions[A].quantity == 100 and exmark.state.cash == 100
    released = apply_bonus_shares(exmark.state, event_id="oneforone", instrument=A, record_date=DAY,
        listing_date=listing, ratio=1, session=listing, known_at="2024-12-31T23:59:59+08:00",
        evidence_sha256=SHA, calendar=CAL, price=5)
    assert mark_to_close(released.state, session=listing, prices={A: 5}, calendar=CAL).equity_lower_bound == 1100
    assert released.state.positions[A].quantity == 200


def test_cash_receivable_is_in_nav_but_cannot_be_spent_before_payment():
    state = PortfolioState(D(0), {A: Position(100, last_mark=D(10), last_mark_date=DAY)})
    state = mark_to_close(state, session=DAY, prices={A: 10}, calendar=CAL).state
    registered = register_dividend_entitlement(state, event_id="cash", instrument=A, record_date=DAY,
        ex_date=NEXT, pay_date="2025-01-06", cash_per_share=1, session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL, tax_rate=0)
    marked = mark_to_close(registered.state, session=NEXT, prices={A: 9}, calendar=CAL)
    assert marked.equity_lower_bound == 1000 and marked.state.cash == 0
    assert marked.claims[0]["spendable"] is False
    assert PortfolioState.from_dict(marked.state.to_dict()).entitlements == marked.state.entitlements


def test_pending_bonus_exposure_is_not_bought_a_second_time_during_rebalance():
    state = PortfolioState(D(0), {A: Position(100, last_mark=D(10), last_mark_date=DAY)})
    state = mark_to_close(state, session=DAY, prices={A: 10}, calendar=CAL).state
    registered = register_bonus_entitlement(state, event_id="bonus2", instrument=A, record_date=DAY,
        ex_date=NEXT, listing_date="2025-01-06", ratio=1, session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL, price=5)
    # Target half the total assets in A. Its frozen 100 pending shares already
    # represent that half, so all currently tradable old shares can be sold.
    result = execute(registered.state, day=NEXT, targets={A: D(".5")}, quotes={A: quote(NEXT, "5", "5")})
    assert result.opening_equity == 1000
    assert A not in result.state.positions and result.trades[0]["side"] == "SELL"
    assert result.trades[0]["quantity"] == 100
    assert result.state.entitlements["bonus2"]["status"] == "pending"


def test_single_order_and_risk_warning_daily_caps_are_not_confused():
    policy = ExecutionPolicy(slippage_bps=0)
    result = execute(PortfolioState(D(200000)), targets={A: 1}, quotes={A: quote(price=".10", preclose=".10")}, policy=policy)
    assert result.trades[0]["quantity"] == 1000000
    assert result.trades[0]["unfilled_quantity"] == 1000000
    warning = replace(info(), is_st=True)
    first = execute(targets={A: 1}, quotes={A: quote(price=".10", preclose=".10")}, metadata={A: warning}, policy=policy)
    assert first.trades[0]["quantity"] == 500000
    second = execute(first.state, targets={A: 1}, quotes={A: quote(price=".10", preclose=".10")}, metadata={A: warning}, policy=policy)
    assert not second.trades and "daily 500000" in second.orders[0]["reason"]


def test_finalized_record_date_snapshot_prevents_later_same_day_retrading_or_rewriting():
    state = execute().state
    closed = mark_to_close(state, session=DAY, prices={A: 10}, calendar=CAL)
    with pytest.raises(ValueError, match="finalized closing"):
        execute(closed.state, targets={})
    altered = PortfolioState.from_dict(closed.state.to_dict())
    altered.positions[A].quantity += 100
    with pytest.raises(ValueError, match="snapshot cannot be overwritten"):
        mark_to_close(altered, session=DAY, prices={A: 10}, calendar=CAL)


def test_zero_holder_bonus_events_do_not_invent_a_security_valuation_requirement():
    state = mark_to_close(PortfolioState(D(100)), session=DAY, prices={}, calendar=CAL).state
    registered = register_bonus_entitlement(state, event_id="not_a_holder", instrument=A, record_date=DAY,
        ex_date=NEXT, listing_date="2025-01-06", ratio=1, session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL)
    assert mark_to_close(registered.state, session=NEXT, prices={}, calendar=CAL).equity_lower_bound == 100


def test_cash_and_bonus_ex_adjustment_of_a_stale_mark_is_order_independent_and_never_a_fill():
    state = PortfolioState(D(0), {A: Position(100, last_mark=D(10), last_mark_date=DAY)})
    state = mark_to_close(state, session=DAY, prices={A: 10}, calendar=CAL).state
    common = dict(instrument=A, record_date=DAY, ex_date=NEXT, session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL)
    def cash(current):
        return register_dividend_entitlement(current, event_id="cash_ex", pay_date="2025-01-06", cash_per_share=1, **common).state
    def bonus(current):
        return register_bonus_entitlement(current, event_id="bonus_ex", listing_date="2025-01-06", ratio=1, **common).state
    for adjusted in (cash(bonus(state)), bonus(cash(state))):
        marked = mark_to_close(adjusted, session=NEXT, prices={}, calendar=CAL)
        assert marked.equity_lower_bound == 980  # Previous1000 minus conservative dividend tax20.
        assert marked.state.positions[A].last_mark == D("4.5")
        assert marked.state.positions[A].last_mark_date == DAY
        assert marked.positions[0]["last_mark_basis"] == "corporate_action_adjusted_carried_mark"
        assert not marked.valuation_complete
        # Its derived mark cannot authorize an execution without a fresh quote.
        result = execute(adjusted, day=NEXT, targets={}, quotes={A: replace(quote(NEXT), open=None, tradestatus="suspended")})
        assert not result.trades and result.state.positions[A].quantity == 100


def test_later_dividends_on_an_uncertain_merge_tail_remain_frozen_but_preserve_the_upper_bound():
    state = PortfolioState(D(0), {A: Position(101, last_mark=D(10), last_mark_date="2024-12-31")})
    merged = apply_share_exchange(state, action_id="tail", from_instrument=A, to_instrument=B, ratio=".62",
        session=DAY, tradable_from=DAY, confirmed_known_at="2024-12-31T23:59:59+08:00",
        evidence_sha256=SHA, calendar=CAL, successor_price=16).state
    closed = mark_to_close(merged, session=DAY, prices={B: 16}, calendar=CAL).state
    registered = register_dividend_entitlement(closed, event_id="later_cash", instrument=B, record_date=DAY,
        ex_date=NEXT, pay_date="2025-01-06", cash_per_share=1, session=NEXT,
        known_at="2024-12-31T23:59:59+08:00", evidence_sha256=SHA, calendar=CAL).state
    marked = mark_to_close(registered, session=NEXT, prices={B: 15}, calendar=CAL)
    # Lower:62*15 + net49.60; upper adds possible1*15 + net0.80 on that share.
    assert marked.equity_lower_bound == D("979.60")
    assert marked.equity_upper_bound == D("995.40")
    assert marked.state.cash == 0 and marked.state.claims["tail"]["cash_upper_bound"] == "0.80"


def test_live_closing_auction_is_not_treated_as_continuous_last_price_execution():
    live = replace(quote(), execution_price=D("10.50"), execution_price_basis="live_observed_quote", asof=DAY + "T14:58:40+08:00")
    result = execute(quotes={A: live}, now=DAY + "T14:59:00+08:00")
    assert not result.trades and "outside continuous" in result.orders[0]["reason"]


def test_invalid_target_calendar_and_state_data_fail_without_partial_mutation():
    with pytest.raises(ValueError, match="sum"):
        execute(targets={A: 2})
    with pytest.raises(ValueError, match="finite"):
        execute(targets={A: float("nan")})
    with pytest.raises(ValueError, match="integer"):
        Position(1.5)
    with pytest.raises(ValueError, match="preceding"):
        execute_target_weights(PortfolioState.initial(), session=DAY, signal_date=DAY, targets={}, quotes={}, metadata={}, calendar=CAL)
