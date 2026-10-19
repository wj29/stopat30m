"""Auditable attribution of an existing, immutable A-share experiment.

This module does not select parameters or simulate a counterfactual portfolio.
Its cost add-backs keep the original quantities and future holdings unchanged.
Overlapping forward-return diagnostics are predictive statistics, not NAVs.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import PackageHistoricalPool
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, read_qlib_field


def _sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _money(value):
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _sum(values):
    return sum((Decimal(str(value)) for value in values), Decimal(0))


def _write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")


def verify_artifact(directory):
    """Verify every declared result file before reading any ledger contents."""
    root = Path(directory).resolve()
    path = root / "manifest.json"
    payload = json.loads(path.read_text())
    if not payload.get("files"):
        raise ValueError("Artifact has no checksummed result inventory")
    for name, digest in payload["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe artifact inventory path")
        file = root / relative
        if file.is_symlink() or not file.is_file() or _sha(file) != digest:
            raise ValueError(f"Artifact checksum mismatch: {name}")
    return payload, {"path": str(path), "sha256": _sha(path), "verified_files": len(payload["files"])}


def classify_trades(trades, signals, positions, calendar):
    """Distinguish actual retained holdings from exits and newly selected names.

    A retained target without an actual holding is an attempted entry, rather
    than repeated resizing of an existing position. This matters under lots.
    """
    result = trades.copy()
    result["trade_role"] = "unclassified"
    result["retained_target"] = False
    result["previous_close_quantity"] = 0
    prior_positions = {(str(row.session), row.instrument): int(row.quantity)
                       for row in positions.itertuples()}
    selected = signals[signals.selected.astype(str).str.lower().eq("true")]
    schedules = signals.session.drop_duplicates().sort_values().tolist()
    previous_targets = set()
    day_index = {day: index for index, day in enumerate(calendar)}
    for day in schedules:
        current = set(selected[selected.session.eq(day)].instrument)
        location = day_index[day]
        preceding = calendar[location - 1] if location else None
        for index, trade in result[result.session.eq(day)].iterrows():
            quantity = prior_positions.get((preceding, trade.instrument), 0)
            retained = trade.instrument in previous_targets and trade.instrument in current
            if retained and quantity > 0:
                role = "retained_holding_resize"
            elif trade.side == "BUY" and trade.instrument in current:
                role = "new_target_entry" if trade.instrument not in previous_targets else "retained_target_first_fill"
            elif trade.side == "SELL" and trade.instrument not in current:
                role = "target_exit"
            else:
                role = "other"
            result.loc[index, ["trade_role", "retained_target", "previous_close_quantity"]] = [role, retained, quantity]
        previous_targets = current
    return result


def ledger_cost_attribution(trades, curves, signals, orders, positions, policy):
    """Exact cent sums and cost-free accounting add-backs of one fixed ledger."""
    if curves.empty or curves.date.duplicated().any():
        raise ValueError("A strategy needs a unique observed daily curve")
    curves = curves.sort_values("date")
    initial = Decimal(str(policy["initial_cash"]))
    commission_rate = Decimal(str(policy["commission_rate"]))
    minimum = Decimal(str(policy["minimum_commission"]))
    slippage_bps = Decimal(str(policy["slippage_bps"]))
    classified = classify_trades(trades, signals, positions, curves.date.tolist())
    fees = {key: _sum(trades[key]) for key in ("commission", "transfer_fee", "stamp_tax", "slippage_cost")}
    curve_columns = {"commission": "commission_cny", "transfer_fee": "transfer_fee_cny",
                     "stamp_tax": "stamp_tax_cny", "slippage_cost": "slippage_cny"}
    for field, column in curve_columns.items():
        if fees[field] != _sum(curves[column]):
            raise ValueError(f"Curve/trade cost reconciliation failed: {field}")
    proportional = Decimal(0)
    commission_mismatches = 0
    for row in trades.itertuples():
        proportional_order = _money(Decimal(str(row.notional)) * commission_rate)
        proportional += proportional_order
        commission_mismatches += Decimal(str(row.commission)) != max(proportional_order, minimum)
        notional = _money(Decimal(str(row.quantity)) * Decimal(str(row.execution_price)))
        slippage = _money(Decimal(str(row.quantity)) * abs(Decimal(str(row.execution_price)) - Decimal(str(row.reference_price))))
        if notional != Decimal(str(row.notional)) or slippage != Decimal(str(row.slippage_cost)):
            raise ValueError("Trade quantity/price/notional/slippage ledger does not reconcile")
        if _sum([row.commission, row.transfer_fee, row.stamp_tax]) != Decimal(str(row.total_fee)):
            raise ValueError("Slippage must not be included a second time in total_fee")
    if commission_mismatches:
        raise ValueError("Recorded commissions differ from the declared rounded per-order policy")
    notional = _sum(trades.notional)
    reference_notional = _sum(Decimal(str(row.quantity)) * Decimal(str(row.reference_price)) for row in trades.itertuples())
    scheduled_slippage = reference_notional * slippage_bps / Decimal(10000)
    net_profit = Decimal(str(curves.iloc[-1].equity_cny)) - initial
    direct_fees = fees["commission"] + fees["transfer_fee"] + fees["stamp_tax"]
    total_friction = direct_fees + fees["slippage_cost"]
    numeric = trades.copy()
    for key in ("notional", "commission", "slippage_cost", "reference_price", "execution_price"):
        numeric[key] = pd.to_numeric(numeric[key])
    avg_equity = float(pd.to_numeric(curves.equity_cny).mean())
    cash_fraction = pd.to_numeric(curves.cash_cny) / pd.to_numeric(curves.equity_cny)
    traded_notional = float(notional)
    roles = {}
    for role, frame in classified.groupby("trade_role"):
        roles[role] = {"trade_count": len(frame), "notional_cny": str(_sum(frame.notional)),
                       "commission_cny": str(_sum(frame.commission)),
                       "all_friction_cny": str(_sum(frame.commission) + _sum(frame.transfer_fee)
                                               + _sum(frame.stamp_tax) + _sum(frame.slippage_cost))}
    thresholds = {}
    for threshold in (100, 500, 1000, 2000):
        selected = numeric[numeric.notional < threshold]
        thresholds[str(threshold)] = {"trade_count": len(selected), "count_fraction": len(selected) / max(1, len(trades)),
                                      "notional_cny": str(_sum(selected.notional))}
    per_trade_bps = ((numeric.execution_price / numeric.reference_price - 1).abs() * 10000)
    rejection_reasons = orders[orders.status.eq("rejected")].groupby("reason", dropna=False).size().to_dict()
    rejected = orders[orders.status.eq("rejected")]
    below_board_min = sum(int(float(row.requested_quantity)) < (200 if str(row.instrument).startswith("SH688") else 100)
                          for row in rejected.itertuples())
    report = {
        "currency": "CNY", "trade_count": len(trades), "observed_sessions": len(curves),
        "period": {"start": curves.iloc[0].date, "end": curves.iloc[-1].date},
        "initial_equity_cny": str(initial), "final_equity_cny": str(curves.iloc[-1].equity_cny),
        "net_profit_cny": str(net_profit), "net_total_return": float(net_profit / initial),
        "fixed_ledger_pnl_bridge_cny": {"reference_fill_before_direct_fees": str(net_profit + total_friction),
                                      "less_actual_slippage": str(fees["slippage_cost"]),
                                      "less_commission": str(fees["commission"]),
                                      "less_transfer_fee": str(fees["transfer_fee"]),
                                      "less_sell_stamp_tax": str(fees["stamp_tax"]), "net_profit": str(net_profit)},
        "fixed_ledger_addback_is_not_a_counterfactual_rerun": True,
        "fixed_ledger_addback_limitations": "Original traded quantities and subsequent holdings stay fixed. Saved costs are not reinvested; cash, integer-lot feasibility and future portfolio choices can change in a full zero-cost rerun. Slippage is already embedded in execution prices, and is not debited again as a fee.",
        "costs": {**{key + "_cny": str(value) for key, value in fees.items()},
                  "total_direct_fees_cny": str(direct_fees), "total_friction_cny": str(total_friction),
                  "proportional_commission_without_minimum_cny": str(proportional),
                  "minimum_commission_excess_cny": str(fees["commission"] - proportional),
                  "at_minimum_commission_order_count": sum(Decimal(str(value)) == minimum for value in trades.commission),
                  "actual_slippage_bps_of_reference_notional": float(fees["slippage_cost"] / reference_notional * 10000) if reference_notional else None,
                  "declared_slippage_budget_cny_before_tick_rounding": str(scheduled_slippage),
                  "adverse_tick_rounding_excess_cny": str(fees["slippage_cost"] - scheduled_slippage),
                  "actual_per_trade_slippage_bps_quantiles": {str(q): float(per_trade_bps.quantile(q)) for q in (0, .5, .9, 1)}},
        "turnover": {"both_sides_traded_notional_cny": str(notional), "reference_notional_cny": str(reference_notional),
                     "average_observed_equity_cny": avg_equity, "both_sides_notional_over_average_equity": traded_notional / avg_equity,
                     "annualized_both_sides_notional_over_average_equity": traded_notional / avg_equity * 252 / len(curves),
                     "annualized_one_way_turnover": traded_notional / avg_equity * .5 * 252 / len(curves)},
        "execution": {"rejected_order_count": len(rejected), "rejected_order_reasons": {str(k): int(v) for k, v in rejection_reasons.items()},
                      "rejected_requested_quantity_below_board_buy_minimum": int(below_board_min),
                      "below_board_minimum_count_is_not_a_claim_about_all_sell_eligibility": True,
                      "mean_cash_cny": float(pd.to_numeric(curves.cash_cny).mean()), "mean_cash_fraction": float(cash_fraction.mean()),
                      "mean_risky_exposure_fraction": float((1 - cash_fraction).mean()),
                      "mean_observed_position_count": float(positions.groupby("session").size().reindex(curves.date, fill_value=0).mean()),
                      "small_trade_notional_thresholds_cny": thresholds},
        "trade_roles": roles,
    }
    return report, classified


def signal_stability(signals):
    selected = signals[signals.selected.astype(str).str.lower().eq("true")]
    sets = selected.groupby("session").instrument.agg(set).sort_index()
    rows = []
    previous = None
    for day, current in sets.items():
        if previous is not None:
            intersection = len(previous & current)
            rows.append({"session": day, "previous_target_count": len(previous), "current_target_count": len(current),
                         "retained_target_count": intersection, "new_target_count": len(current - previous),
                         "exited_target_count": len(previous - current),
                         "retained_fraction_of_current_targets": intersection / max(1, len(current))})
        previous = current
    frame = pd.DataFrame(rows)
    return {"scheduled_signal_count": len(sets), "successive_signal_pairs": len(frame),
            "mean_retained_target_count": float(frame.retained_target_count.mean()) if len(frame) else None,
            "mean_new_target_count": float(frame.new_target_count.mean()) if len(frame) else None,
            "mean_retained_fraction": float(frame.retained_fraction_of_current_targets.mean()) if len(frame) else None}, frame


def _hac_summary(values, lag):
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if not len(values):
        return {"count": 0, "mean": None, "hac_standard_error": None, "mean_95pct_interval": None}
    centered = values - values.mean()
    variance = float(centered @ centered / len(values))
    for offset in range(1, min(lag, len(values) - 1) + 1):
        covariance = float(centered[offset:] @ centered[:-offset] / len(values))
        variance += 2 * (1 - offset / (lag + 1)) * covariance
    standard_error = np.sqrt(max(0, variance) / len(values))
    return {"count": len(values), "mean": float(values.mean()), "median": float(np.median(values)),
            "positive_fraction": float((values > 0).mean()), "hac_lag_sessions": lag,
            "hac_standard_error": float(standard_error),
            "mean_95pct_interval": [float(values.mean() - 1.96 * standard_error), float(values.mean() + 1.96 * standard_error)]}


def predictive_daily_statistics(scores, adjusted_open, calendar, *, end, horizons=(1, 5, 10, 20)):
    """Align prior-close scores with next-open future returns, never same open."""
    if scores.duplicated(["datetime", "instrument"]).any():
        raise ValueError("Duplicate predictions cannot enter cross-sectional IC")
    locations = {day: index for index, day in enumerate(calendar)}
    cutoff = locations[end]
    rows = []
    for day, group in scores.groupby("datetime", sort=True):
        index = locations[str(day)]
        for horizon in horizons:
            if index + horizon + 1 > cutoff:
                continue
            symbols = [symbol for symbol in group.instrument if symbol in adjusted_open.columns]
            entry = adjusted_open.iloc[index + 1].reindex(symbols)
            exit_price = adjusted_open.iloc[index + horizon + 1].reindex(symbols)
            target = exit_price / entry - 1
            predicted = group.set_index("instrument").score.reindex(symbols)
            frame = pd.DataFrame({"score": predicted, "label": target}).replace([np.inf, -np.inf], np.nan).dropna()
            frame = frame[(entry.reindex(frame.index) > 0) & (exit_price.reindex(frame.index) > 0)]
            if len(frame) < 20 or frame.score.nunique() < 2 or frame.label.nunique() < 2:
                continue
            # Stable score ordering gives deterministic disjoint quintiles.
            frame = frame.rename_axis(None).assign(instrument=frame.index).sort_values(["score", "instrument"], ascending=[False, True])
            quintiles = np.array_split(frame.label.to_numpy(), 5)
            rows.append({"signal_date": str(day), "entry_date": calendar[index + 1],
                         "exit_date": calendar[index + horizon + 1], "year": calendar[index + 1][:4],
                         "horizon_sessions": horizon, "count": len(frame), "unlabelled_score_count": len(group) - len(frame),
                         "pearson_ic": float(frame.score.corr(frame.label)),
                         "rank_ic": float(frame.score.rank().corr(frame.label.rank())),
                         "top20_gross_forward_return": float(frame.label.iloc[:20].mean()),
                         "universe_gross_forward_return": float(frame.label.mean()),
                         "top20_excess_gross_forward_return": float(frame.label.iloc[:20].mean() - frame.label.mean()),
                         **{f"quintile_{i + 1}_gross_forward_return": float(values.mean()) for i, values in enumerate(quintiles)},
                         "top_minus_bottom_quintile_return": float(quintiles[0].mean() - quintiles[-1].mean())})
    return pd.DataFrame(rows)


def summarize_predictive_statistics(frame):
    result = {}
    if frame.empty:
        return result
    for period, data in [("all", frame), *((str(year), group) for year, group in frame.groupby("year"))]:
        result[period] = {}
        for horizon, group in data.groupby("horizon_sessions"):
            keys = [key for key in frame.columns if key.endswith("return") or key in ("pearson_ic", "rank_ic")]
            result[period][str(horizon)] = {"sessions": len(group), "sample_count": int(group["count"].sum()),
                "first_signal_date": group.signal_date.min(), "last_signal_date": group.signal_date.max(),
                "statistics": {key: _hac_summary(group[key], int(horizon)) for key in keys}}
    return result


def prediction_rank_stability(scores):
    rows = []
    previous = None
    for day, group in scores.groupby("datetime", sort=True):
        today = group.set_index("instrument").score.sort_index()
        if previous is not None:
            overlap = pd.concat([previous.rename("previous"), today.rename("today")], axis=1).dropna()
            if len(overlap) >= 20:
                rows.append({"date": day, "year": day[:4], "count": len(overlap),
                             "rank_autocorrelation": float(overlap.previous.rank().corr(overlap.today.rank()))})
        previous = today
    frame = pd.DataFrame(rows)
    return {period: _hac_summary(data.rank_autocorrelation, 5) for period, data in
            [("all", frame), *((year, group) for year, group in frame.groupby("year"))]} if len(frame) else {}, frame


def run_diagnostics(experiment_dir, training_dir, output_dir, *, make_plot=True):
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Diagnostics output exists; preserve the previous result")
    root, training = Path(experiment_dir).resolve(), Path(training_dir).resolve()
    manifest, experiment_receipt = verify_artifact(root)
    _, training_receipt = verify_artifact(training)
    protocol = json.loads((root / "protocol.json").read_text())
    report = json.loads((root / "report.json").read_text())
    if report.get("training_origin", {}).get("sha256") != training_receipt["sha256"]:
        raise ValueError("Diagnostics training scores are not the A2-bound score artifact")
    for receipt in manifest["bound_inputs"].values():
        if _sha(receipt["path"]) != receipt["sha256"]:
            raise ValueError("A2 bound source input changed")
    # Read money as text so the bridge sums exact posted cents, not float noise.
    tables = {name: pd.read_csv(root / (name + ".csv"), dtype=str).fillna("")
              for name in ("curves", "trades", "orders", "signals", "positions")}
    result = {"schema_version": 1, "purpose": "Post-result diagnostics; not untouched OOS model selection", "inputs": {
        "experiment": experiment_receipt, "training": training_receipt}, "strategy_cost_attribution": {}, "signal_stability": {}}
    all_classified, all_stability = [], []
    for strategy in sorted(tables["curves"].strategy.unique()):
        data = {name: table[table.strategy.eq(strategy)].copy() for name, table in tables.items()}
        attribution, classified = ledger_cost_attribution(data["trades"], data["curves"], data["signals"], data["orders"], data["positions"], protocol["execution_policy"])
        stability, stability_frame = signal_stability(data["signals"])
        result["strategy_cost_attribution"][strategy] = attribution
        result["signal_stability"][strategy] = stability
        all_classified.append(classified.assign(strategy=strategy))
        all_stability.append(stability_frame.assign(strategy=strategy))
    training_protocol = json.loads((training / "protocol.json").read_text())
    snapshot_manifest = Path(manifest["bound_inputs"]["snapshot_manifest"]["path"])
    dataset = snapshot_manifest.parent / "dataset"
    calendar_path = dataset / "calendars/day.txt"
    membership_path = dataset / "instruments/csi300.txt"
    if _sha(calendar_path) != training_protocol["used_files_sha256"]["calendars/day.txt"]:
        raise ValueError("Predictive labels calendar differs from frozen training")
    if _sha(membership_path) != training_protocol["membership_sha256"]:
        raise ValueError("Historical index membership differs from frozen training")
    calendar = calendar_path.read_text().splitlines()
    scores = pd.read_csv(training / "predictions.csv", dtype={"datetime": str, "instrument": str}, float_precision="round_trip")
    spec = protocol["spec"]
    # Scores before OOS are needed for the first next-open signal; its entry is
    # the first OOS session, rather than an in-sample observation.
    first = next(day for day in calendar if day >= spec["oos_start"])
    first_signal = calendar[calendar.index(first) - 1]
    scores = scores[scores.datetime.between(first_signal, spec["oos_end"])].copy()
    pool = PackageHistoricalPool(membership_path, start=first_signal, end=spec["oos_end"])
    eligible = {day: set(pool.members_on(day)) for day in scores.datetime.unique()}
    scores = scores[[row.instrument in eligible[row.datetime] for row in scores.itertuples()]]
    opens = {}
    for symbol in scores.instrument.unique():
        relative = f"features/{symbol.lower()}/open.day.bin"
        path = dataset / relative
        if _sha(path) != training_protocol["used_files_sha256"][relative]:
            raise ValueError(f"Adjusted label price checksum differs from frozen training: {symbol}")
        opens[symbol] = read_qlib_field(path, calendar)
    adjusted_open = pd.DataFrame(opens, index=calendar)
    predictive = predictive_daily_statistics(scores, adjusted_open, calendar, end=spec["oos_end"])
    result["predictive_statistics"] = summarize_predictive_statistics(predictive)
    rank_summary, rank_frame = prediction_rank_stability(scores)
    result["prediction_rank_autocorrelation"] = rank_summary
    result["predictive_limitations"] = [
        "Scores use the unchanged pre-2023 fitted model. Adjusted forward opens and package historical members match the frozen input hashes.",
        "Future-return entry is next session open; exit is horizon sessions later. Labels with missing/nonpositive endpoint prices are excluded and counts disclosed. No endpoint exceeds the declared OOS end.",
        "Daily horizon returns overlap. Newey-West mean intervals use lag equal to the horizon; these are diagnostic, not a guarantee of independence or future generalization.",
        "Quintile and top20 returns are gross, cross-sectional forward averages, not feasible integer portfolios. Historical interval membership is not full announcement-time PIT evidence.",
        "2025 and 2026 results have now been inspected and cannot serve as untouched holdouts for subsequent parameter selection.",
    ]
    output.mkdir(parents=True)
    _write(output / "diagnostics.json", result)
    pd.concat(all_classified, ignore_index=True).to_csv(output / "classified_trades.csv", index=False)
    pd.concat(all_stability, ignore_index=True).to_csv(output / "signal_stability.csv", index=False)
    predictive.to_csv(output / "predictive_daily.csv", index=False)
    rank_frame.to_csv(output / "daily_rank_autocorrelation.csv", index=False)
    if make_plot:
        _plot(result, output / "cost_attribution.png")
    _write(output / "manifest.json", {"schema_version": 1, "inputs": result["inputs"],
        "implementation": {"path": str(Path(__file__).resolve()), "sha256": _sha(__file__)},
        "files": {file.name: _sha(file) for file in sorted(output.iterdir()) if file.is_file()}})
    return result


def _plot(report, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    strategies = ["lightgbm", "momentum"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), layout="constrained")
    for axis, strategy in zip(axes, strategies):
        data = report["strategy_cost_attribution"][strategy]
        bridge = data["fixed_ledger_pnl_bridge_cny"]
        labels = ["Reference\nfill PnL", "Slippage", "Commission", "Other fees", "Net PnL"]
        values = [float(bridge["reference_fill_before_direct_fees"]), -float(bridge["less_actual_slippage"]),
                  -float(bridge["less_commission"]), -float(bridge["less_transfer_fee"]) - float(bridge["less_sell_stamp_tax"]),
                  float(bridge["net_profit"])]
        running = values[0]
        for index, value in enumerate(values):
            if index in (0, 4):
                base = 0
            else:
                base = running + value
                running += value
            axis.bar(index, abs(value), bottom=base, color="#2563eb" if index in (0, 4) else "#dc2626", width=.7)
            axis.text(index, base + abs(value) + 350, f"{value:,.0f}", ha="center", fontsize=9)
        axis.set_xticks(range(5), labels, fontsize=9)
        axis.set_ylabel("CNY over the full 431-session ledger")
        axis.set_title(strategy)
        axis.grid(axis="y", alpha=.2)
    fig.suptitle("Fixed original quantities: accounting cost attribution, not a zero-cost rerun")
    fig.savefig(path, dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--training", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    diagnosis = run_diagnostics(args.experiment, args.training, args.output_dir)
    print(json.dumps({"output_dir": args.output_dir, "strategies": list(diagnosis["strategy_cost_attribution"])}, ensure_ascii=False))
