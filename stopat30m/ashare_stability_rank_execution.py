"""Secondary-only rank execution using the already declared reduced quantities.

The legacy adapter and completed primary experiments remain byte-identical.
Only the entry weight numerator is corrected to its actual budget-reduced
quantity; rank selection, original frozen planning quantities, fees, queues,
retained shares and the underlying ledger are unchanged.
"""
from decimal import Decimal, ROUND_FLOOR, localcontext
from typing import Mapping, Sequence
from stopat30m.backtest import ashare_execution as ex
from stopat30m.ashare_optimization_execution import (
    RankBufferPolicy, RankBufferPlan, RankBufferExecution, build_rank_buffer_plan,
    _board_quantity,
)

POLICY = "a5-secondary-rank-buffer-use-actual-budget-reduced-quantities-v1"


def execute_rank_buffer_plan(state: ex.PortfolioState, *, plan: RankBufferPlan, session: str,
        quotes: Mapping[str, ex.OpeningQuote], metadata: Mapping[str, ex.SecurityInfo],
        calendar: Sequence[str], policy=None, now=None, verified_suspended: Sequence[str] = ()):
    """Execute exits/new names once; never submit retained shares to reweighting.

    A pure engine probe provides exactly the original opening NAV and validates
    held stale-price limits. Its hypothetical orders are discarded; only the
    second engine call below produces the returned cash/trade ledger.  No probe
    changes its input state or performs I/O.
    """
    policy = policy or ex.ExecutionPolicy()
    if plan.signal_date >= session or plan.signal_date not in calendar:
        raise ValueError("A rank-buffer plan must be frozen on a preceding real exchange session")
    if set(plan.retained) & {inst for inst, *_ in plan.entries}:
        raise ValueError("Retained and new-entry sleeves cannot overlap")
    active = {inst for inst, position in state.positions.items() if position.quantity}
    active.update(row["instrument"] for row in state.entitlements.values()
        if row.get("kind") == "bonus_shares" and row.get("status") == "pending"
        and ex.decimal(row["quantity"]) > 0)
    if active-set(plan.retained)-set(plan.exits):
        raise ValueError("Portfolio exposure changed after the rank-buffer plan was frozen")
    original = ex._advance(state, session, calendar)
    targets_for_probe = {inst: 0 for inst, *_ in plan.entries}
    full_probe = ex.execute_target_weights(original, session=session, signal_date=plan.signal_date,
        targets=targets_for_probe, quotes=quotes, metadata=metadata, calendar=calendar,
        policy=policy, now=now, verified_suspended=verified_suspended)
    retained = set(plan.retained)
    sleeve = ex.PortfolioState.from_dict(original.to_dict())
    kept_positions = {inst: sleeve.positions.pop(inst) for inst in sorted(retained) if inst in sleeve.positions}
    kept_entitlements = {identity: sleeve.entitlements.pop(identity) for identity, row in list(sleeve.entitlements.items())
        if row.get("instrument") in retained}
    kept_claims = {identity: sleeve.claims.pop(identity) for identity, row in list(sleeve.claims.items())
        if row.get("instrument") in retained}
    sleeve_probe = ex.execute_target_weights(sleeve, session=session, signal_date=plan.signal_date,
        targets=targets_for_probe, quotes=quotes, metadata=metadata, calendar=calendar,
        policy=policy, now=now, verified_suspended=verified_suspended)
    equity = sleeve_probe.opening_equity
    references, missing = {}, []
    for inst, quantity, signal_price, _ in plan.entries:
        try:
            references[inst] = ex._reference(quotes[inst], session, policy, now=now)
        except (KeyError, ValueError):
            # Reserve its original allocation rather than using an unavailable
            # opening to resize or substitute another stock upward.
            references[inst] = ex.decimal(signal_price)
            missing.append(inst)
    wanted = sum((quantity * references[inst] for inst, quantity, _, _ in plan.entries), Decimal(0))
    scale = min(Decimal(1), equity/wanted) if wanted else Decimal(1)
    quantities = {inst: _board_quantity(Decimal(quantity)*scale, board)
        for inst, quantity, _, board in plan.entries}
    weights = {}
    # Extra division precision avoids rounding an exact planned 100-share lot
    # below its integer boundary in the engine's usual 28-digit multiplication.
    # ROUND_FLOOR never inflates a share instruction or creates leverage.
    with localcontext() as context:
        context.prec = 80
        context.rounding = ROUND_FLOOR
        for inst, quantity, _, _ in plan.entries:
            weights[inst] = Decimal(quantities[inst])*references[inst]/equity if equity else Decimal(0)
    # Conservative, explicitly audited fallback for a sum rounded up at the
    # original engine's Decimal precision. No incumbent is involved.
    rounding_reduction = Decimal(0)
    if sum(weights.values(), Decimal(0)) > 1:
        key = max(weights, key=lambda inst: (weights[inst], inst))
        rounding_reduction = sum(weights.values(), Decimal(0))-1+Decimal(len(weights)+1)*Decimal("1e-27")
        weights[key] = max(Decimal(0), weights[key]-rounding_reduction)
    if sum(weights.values(), Decimal(0)) > 1:
        raise ValueError("Scaled entry quantities exceed the available sleeve budget")
    result = ex.execute_target_weights(sleeve, session=session, signal_date=plan.signal_date,
        targets=weights, quotes=quotes, metadata=metadata, calendar=calendar,
        policy=policy, now=now, verified_suspended=verified_suspended)
    if any(row["instrument"] in retained for row in result.trades):
        raise ValueError("A retained share escaped the isolated execution sleeve")
    result.state.positions.update(kept_positions)
    result.state.entitlements.update(kept_entitlements)
    result.state.claims.update(kept_claims)
    result.opening_equity = full_probe.opening_equity
    expected_cash = original.cash + sum((ex.decimal(row["cash_change"]) for row in result.trades), Decimal(0))
    if result.state.cash != expected_cash or result.state.cash < 0:
        raise ValueError("Merged sleeve cash does not reconcile to original engine fills")
    if result.state.applied_actions != original.applied_actions or result.state.holdings_history != original.holdings_history:
        raise ValueError("Execution may not rewrite corporate-action or record-date history")
    return RankBufferExecution(result, {"policy_version": POLICY, "plan": plan.to_dict(),
        "sleeve_opening_equity_cny": str(equity), "full_opening_equity_cny": str(full_probe.opening_equity),
        "opening_budget_scale": str(scale), "opening_quantities_after_budget_reduction": quantities,
        "submitted_target_weights": {inst: str(weight) for inst, weight in weights.items()},
        "targets_use_budget_reduced_quantities": True,
        "unavailable_opening_references": missing, "weight_sum": str(sum(weights.values(), Decimal(0))),
        "weight_rounding_reduction": str(rounding_reduction), "opening_replacements": [],
        "retained_shares": {inst: position.quantity for inst, position in kept_positions.items()}})
