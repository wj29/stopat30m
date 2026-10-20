from decimal import Decimal as D

import pytest

from stopat30m import ashare_optimization_execution as legacy
from stopat30m import ashare_stability_rank_execution as fixed
from stopat30m.backtest import ashare_execution as ex

SIGNAL, DAY = "2024-12-31", "2025-01-02"
CALENDAR = [SIGNAL, DAY]
KEEP, A, B = "SH600000", "SH600001", "SH600002"


def inputs(state, symbols, *, price="10", boards=None, previous_equity="10000"):
    boards = boards or {s: "sse_main" for s in symbols}
    prices = {s: price for s in symbols}
    plan = fixed.build_rank_buffer_plan(state, signal_date=SIGNAL, ranked_instruments=symbols,
        raw_signal_prices=prices, boards=boards, signal_equity=previous_equity,
        policy=fixed.RankBufferPolicy(top_k=len(symbols), keep_rank=len(symbols), require_signal_affordability=True))
    quotes = {s: ex.OpeningQuote(DAY, D(price), D(price), "trading", "synthetic observed opening") for s in symbols}
    metadata = {s: ex.SecurityInfo(s, boards[s], "2000-01-03", False,
        "2024-12-01T23:59:59+08:00", "synthetic prior-known identity") for s in symbols}
    return dict(plan=plan, session=DAY, quotes=quotes, metadata=metadata, calendar=CALENDAR)


def test_retained_overweight_cannot_turn_unscaled_entry_plan_into_leverage():
    state = ex.PortfolioState(D("1000"), {KEEP: ex.Position(900, last_mark=D(10), last_mark_date=SIGNAL)})
    before = state.to_dict(); kwargs = inputs(state, [KEEP, A, B])
    assert [q for _, q, _, _ in kwargs["plan"].entries] == [300, 300]
    with pytest.raises(ValueError, match="Long-only target weights"):
        legacy.execute_rank_buffer_plan(state, **kwargs)
    result = fixed.execute_rank_buffer_plan(state, **kwargs)
    assert result.audit["opening_quantities_after_budget_reduction"] == {A: 0, B: 0}
    assert {s: D(w) for s, w in result.audit["submitted_target_weights"].items()} == {A: D(0), B: D(0)}
    assert not result.trades and result.state.cash == 1000
    assert result.state.positions[KEEP].quantity == 900 and state.to_dict() == before


@pytest.mark.parametrize("cash", ["1000", "1400", "3000", "5000"])
def test_star_entries_respect_actual_scaled_quantities_and_full_fee_cash(cash):
    a, b = "SH688001", "SH688002"
    state = ex.PortfolioState(D(cash), {KEEP: ex.Position(4500, last_mark=D(2), last_mark_date=SIGNAL)})
    kwargs = inputs(state, [KEEP, a, b], price="2", boards={KEEP: "sse_main", a: "star", b: "star"},
                    previous_equity=str(D(cash) + 9000))
    result = fixed.execute_rank_buffer_plan(state, **kwargs)
    reduced = result.audit["opening_quantities_after_budget_reduction"]
    assert D(result.audit["opening_budget_scale"]) < 1
    assert all(t["quantity"] <= reduced[t["instrument"]] for t in result.trades if t["side"] == "BUY")
    assert sum((D(w) for w in result.audit["submitted_target_weights"].values()), D(0)) <= 1
    assert result.state.cash == state.cash + sum((D(t["cash_change"]) for t in result.trades), D(0))
    assert result.state.cash >= 0 and result.state.positions[KEEP].quantity == 4500
    assert all(D(t["commission"]) >= 5 for t in result.trades)
    assert result.audit["plan"]["entries"] == kwargs["plan"].to_dict()["entries"]


def test_without_budget_reduction_all_fills_fees_and_states_match_legacy():
    state = ex.PortfolioState(D("10000")); kwargs = inputs(state, [A, B])
    old = legacy.execute_rank_buffer_plan(state, **kwargs)
    new = fixed.execute_rank_buffer_plan(state, **kwargs)
    assert D(new.audit["opening_budget_scale"]) == 1
    assert new.state.to_dict() == old.state.to_dict()
    assert new.orders == old.orders and new.trades == old.trades
    assert new.opening_equity == old.opening_equity
