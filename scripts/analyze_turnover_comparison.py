"""Verify and summarize a completed fixed five-variant, three-period A6 run.

This is a read-only ledger analysis. It never fits, simulates, chooses parameters,
updates a paper account, or adds recorded costs back to reported net returns.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import tempfile
import os

import numpy as np
import pandas as pd


VARIANTS = ("baseline", "hold10_cap2", "hold10_cap5", "hold20_cap2", "hold20_cap5")
PERIODS = ("2025", "2026", "continuous")
TERMS = [(0, None), (10, 2), (10, 5), (20, 2), (20, 5)]
KIND = "a6_fixed_lgb_turnover_results"


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024*1024), b""):
            digest.update(block)
    return digest.hexdigest()


def proof(path):
    return {"path": str(Path(path).resolve()), "sha256": sha(path)}


def read_json(path):
    return json.loads(Path(path).read_bytes())


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False)+"\n")


def verify_manifest(root, manifest):
    root = Path(root).resolve()
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("A complete, nonempty artifact file manifest is required")
    actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
        and path != root/"manifest.json"}
    if actual != set(files):
        raise ValueError("Artifact file inventory differs from its manifest")
    for name, digest in files.items():
        path = (root/name).resolve()
        if (not path.is_relative_to(root) or Path(name).is_absolute()
                or not isinstance(digest, str) or len(digest) != 64 or sha(path) != digest):
            raise ValueError("Changed or unsafe artifact: "+name)


def collect_proofs(value, bound, prefix):
    if isinstance(value, dict):
        if "path" in value and "sha256" in value:
            path, digest = value["path"], value["sha256"]
            if not isinstance(path, str) or not Path(path).is_absolute() or not isinstance(digest, str) or len(digest) != 64:
                raise ValueError("Invalid bound original proof: "+prefix)
            bound[prefix] = {"path": str(Path(path).resolve()), "sha256": digest}
        for key, item in value.items():
            collect_proofs(item, bound, prefix+":"+str(key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            collect_proofs(item, bound, prefix+":"+str(index))


def verify_proofs(bound):
    checked = set()
    for item in bound.values():
        identity = item["path"], item["sha256"]
        if identity in checked:
            continue
        if sha(item["path"]) != item["sha256"]:
            raise ValueError("Bound original changed: "+item["path"])
        checked.add(identity)


def read_csv(path):
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def finite(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Recorded economic values must be finite")
    return result


def total(frame, key):
    if frame.empty:
        return Decimal(0)
    if key not in frame:
        raise ValueError("Missing recorded economic field: "+key)
    return sum((finite(value) for value in frame[key] if value != ""), Decimal(0))


def close_enough(left, right, name, tolerance=1e-12):
    if not np.isfinite([float(left), float(right)]).all() or abs(float(left)-float(right)) > tolerance:
        raise ValueError("Recorded metric does not reconcile: "+name)


def path_diagnostics(curve, initial, anchor):
    if curve.empty:
        return {"anchor_date": anchor, "observations": 0}, [anchor], [1.]
    dates = [anchor, *curve.date.tolist()]
    nav = np.r_[1., curve.equity_cny.to_numpy(dtype=float)/float(initial)]
    upper_nav = np.r_[1., curve.equity_upper_bound_cny.to_numpy(dtype=float)/float(initial)]
    if not np.isfinite(nav).all() or (nav <= 0).any() or dates != sorted(set(dates)):
        raise ValueError("NAV must be positive, ordered and uniquely dated")
    if not np.isfinite(upper_nav).all() or (upper_nav < nav).any():
        raise ValueError("Conservative NAV bounds are inconsistent")
    conservative_drawdown = nav/np.maximum.accumulate(upper_nav)-1
    returns = pd.Series(nav[1:]/nav[:-1]-1, index=pd.to_datetime(dates[1:]))
    monthly = (1+returns).groupby(returns.index.to_period("M")).prod()-1
    rolling = {}
    for length in (63, 126, 252):
        values = nav[length:]/nav[:-length]-1 if len(nav) > length else np.array([])
        rolling[str(length)] = {"observations": len(values),
            "minimum": float(values.min()) if len(values) else None,
            "median": float(np.median(values)) if len(values) else None,
            "maximum": float(values.max()) if len(values) else None,
            "positive_fraction": float((values > 0).mean()) if len(values) else None,
            "daily": [{"date": dates[index+length], "net_return": float(value)}
                for index, value in enumerate(values)]}
    return {"anchor_date": anchor, "last_observed_date": dates[-1], "observations": len(curve),
        "net_return": float(nav[-1]-1), "max_drawdown": float(conservative_drawdown.min()),
        "drawdown_basis": "recorded_lower_NAV_divided_by_running_recorded_upper_NAV_peak_including_cash_anchor",
        "lower_NAV_peak_drawdown": float((nav/np.maximum.accumulate(nav)-1).min()),
        "conservative_drawdown_daily": conservative_drawdown.tolist(),
        "monthly_returns": {str(month): float(value) for month, value in monthly.items()},
        "rolling_actual_sessions": rolling,
        "mean_invested_fraction": float(np.mean(1-curve.cash_cny.to_numpy(dtype=float)/curve.equity_cny.to_numpy(dtype=float)))}, dates, nav.tolist()


def cost_audit(summary, trades, orders, actions, config):
    metrics = summary["metrics"]
    sums = {name+"_cny": str(total(trades, name)) for name in
        ("commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost", "notional")}
    for key, value in sums.items():
        if finite(metrics[key]) != finite(value):
            raise ValueError("Cost metric differs from actual fills: "+key)
    if finite(sums["total_fee_cny"]) != sum((finite(sums[key+"_cny"]) for key in
            ("commission", "transfer_fee", "stamp_tax")), Decimal(0)):
        raise ValueError("Fee components do not reconcile")
    minimum, rate = finite(config["minimum_commission_cny"]), finite(config["commission_rate"])
    at_floor = strict_floor = 0
    for row in trades.to_dict("records"):
        notional, commission = finite(row["notional"]), finite(row["commission"])
        if notional <= 0 or row.get("status") != "simulated_fill":
            raise ValueError("Costs must come from positive-notional actual simulated fills")
        proportional = (notional*rate).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
        if commission != max(minimum, proportional):
            raise ValueError("Fill commission differs from declared rate and floor")
        at_floor += commission == minimum
        strict_floor += proportional < minimum
    metric_tax = {key: str(finite(metrics[key])) for key in
        ("recognized_dividend_gross_cash_cny", "recognized_dividend_legacy_rounded_gross_cash_cny",
         "recognized_distribution_tax_reserve_cny", "additional_distribution_tax_reserved_cny",
         "total_distribution_tax_reserved_cny", "cash_distribution_rounding_reserved_cny")}
    tax = extra = rounding = gross = Decimal(0)
    for row in actions.to_dict("records"):
        if row.get("kind") == "cash_dividend" and row.get("status") == "pending":
            tax += finite(row["tax"]); gross += finite(row["gross_cash"])
        elif row.get("kind") == "additional_distribution_tax_reserve":
            extra += finite(row["additional_tax_reserved_cny"])
        elif row.get("kind") == "cash_distribution_rounding_reserve":
            rounding += finite(row["rounding_reserved_cny"])
    for key, value in {"recognized_distribution_tax_reserve_cny": tax,
            "additional_distribution_tax_reserved_cny": extra,
            "total_distribution_tax_reserved_cny": tax+extra,
            "cash_distribution_rounding_reserved_cny": rounding,
            "recognized_dividend_legacy_rounded_gross_cash_cny": gross,
            "recognized_dividend_gross_cash_cny": gross-rounding}.items():
        if finite(metric_tax[key]) != value:
            raise ValueError("Corporate cost does not reconcile: "+key)
    notional = finite(sums["notional_cny"])
    floor_fraction = at_floor/len(trades) if len(trades) else None
    if summary.get("minimum_commission_fill_fraction") != floor_fraction:
        raise ValueError("Recorded minimum commission fraction differs from actual fills")
    rejected = orders.loc[orders.status.eq("rejected")] if not orders.empty else pd.DataFrame()
    return {**sums, "fees_plus_slippage_cny": str(finite(sums["total_fee_cny"])+finite(sums["slippage_cost_cny"])),
        "executed_fill_count": len(trades), "order_record_count": len(orders),
        "rejected_order_count": len(rejected),
        "rejected_reasons": dict(Counter(rejected.reason)) if "reason" in rejected else {},
        "declared_commission_rate": str(rate), "minimum_commission_cny": str(minimum),
        "minimum_commission_fill_count": at_floor, "strictly_floor_binding_fill_count": strict_floor,
        "minimum_commission_fill_fraction": floor_fraction,
        "strictly_floor_binding_fill_fraction": strict_floor/len(trades) if len(trades) else None,
        "commission_actual_notional_bps": str(10000*finite(sums["commission_cny"])/notional) if notional else None,
        "corporate_cash_and_tax_cny": metric_tax,
        "slippage_already_in_fill_prices_not_subtracted_again": True,
        "cost_add_back_used_to_infer_hypothetical_returns": False}


def distribution(values):
    values = np.asarray(values, dtype=float)
    return {"observations": len(values), "mean": float(values.mean()) if len(values) else None,
        "minimum": float(values.min()) if len(values) else None,
        "p25": float(np.percentile(values, 25)) if len(values) else None,
        "median": float(np.median(values)) if len(values) else None,
        "p75": float(np.percentile(values, 75)) if len(values) else None,
        "maximum": float(values.max()) if len(values) else None}


def holding_audit(folder, summary, trades, actions, calendar):
    paths = [folder/(name+".json") for name in ("holding_age", "turnover_plans", "holding_origin_actions")]
    if summary["variant"] == "baseline":
        if any(path.exists() for path in paths):
            raise ValueError("Baseline must retain its original records without added turnover audits")
        return {"status": "not_computed", "reason": "original baseline has no extra holding-origin or turnover-plan audit"}, []
    ages, plans, origins = [read_json(path) for path in paths]
    locations = {day: index for index, day in enumerate(calendar)}
    age_index = {}
    for row in ages:
        day, inst, origin = row["session"], row["instrument"], row["holding_started"]
        key = day, inst
        if key in age_index or day not in locations or origin not in locations or origin > day:
            raise ValueError("Invalid or duplicate actual holding-age observation")
        if row["holding_age_sessions"] != locations[day]-locations[origin]:
            raise ValueError("Holding-age audit differs from actual session calendar")
        age_index[key] = row
    lookup = {}
    for row in origins:
        lookup.setdefault(row["session"], []).append(row)
    sold = []
    for row in trades.to_dict("records"):
        if row.get("side") != "SELL" or row.get("status") != "simulated_fill":
            continue
        day, inst = row["session"], row["instrument"]
        prior = calendar[locations[day]-1] if day in locations and locations[day] else None
        origin = age_index.get((prior, inst), {}).get("holding_started")
        basis = "preceding_finalized_actual_holding_origin" if origin else None
        for event in lookup.get(day, []):
            if event.get("kind") == "inherited_holding_origin" and event["instrument"] == inst:
                # The immutable simulator emits these only from actual bound
                # corporate ledger results; require a corresponding actual event.
                match = [action for action in actions.to_dict("records") if action.get("session") == day and (
                    event.get("basis") == "confirmed_actual_share_exchange" and action.get("kind") == "share_exchange"
                    and action.get("from") == event.get("source_instrument") and action.get("to") == inst
                    and finite(action.get("old_raw_shares", "0")) > 0 and finite(action.get("new_integer_shares", "0")) > 0
                    or event.get("basis") == "actual_bonus_record_date_origin" and action.get("kind") == "bonus_shares"
                    and action.get("instrument") == inst and finite(action.get("record_date_shares", "0")) > 0)]
                if not match:
                    raise ValueError("Inherited holding origin lacks the actual corporate action ledger")
                origin, basis = event["origin"], event["basis"]
        if origin not in locations or day not in locations or origin > day:
            sold.append({"session": day, "instrument": inst, "quantity": row["quantity"],
                "holding_age_sessions": None, "status": "not_computed_unknown_evidenced_origin"})
            continue
        sold.append({"session": day, "instrument": inst, "quantity": row["quantity"],
            "holding_started": origin, "holding_age_sessions": locations[day]-locations[origin],
            "status": "reconstructed_from_bound_actual_holding_and_action_audits", "origin_basis": basis})
    known = [row for row in sold if row["holding_age_sessions"] is not None]
    qsum = sum((finite(row["quantity"]) for row in known), Decimal(0))
    minimum = summary["variant_terms"]["minimum_hold_sessions"]
    counts = {name: sum(len(row[key]) for row in plans) for name, key in
        (("minimum_hold_locked_security_decisions", "minimum_hold_locked"),
         ("cap_forced_retained_security_decisions", "cap_forced_retained"),
         ("exit_intents_without_sell_fill", "exit_intent_without_sell_fill"))}
    for name in ("exit_intent_count", "actual_exit_fill_count", "actual_exit_cleared_count", "rejected_exit_orders"):
        counts[name] = sum(row[name] for row in plans)
    if (counts["exit_intent_count"] != summary["metrics"]["planned_exit_attempt_securities"]
            or counts["actual_exit_fill_count"] != summary["metrics"]["actual_exit_fill_securities"]):
        raise ValueError("Turnover-plan counts differ from saved result metrics")
    return {"status": "computed_from_bound_extra_audits", **counts,
        "scheduled_rebalances": len(plans), "minimum_hold_locked_rebalances": sum(bool(row["minimum_hold_locked"]) for row in plans),
        "cap_forced_retained_rebalances": sum(bool(row["cap_forced_retained"]) for row in plans),
        "forced_retained_overflow_rebalances": sum(row["overflow_count"] > 0 for row in plans),
        "actual_overflow_rebalances": sum(row["actual_overflow_after_execution"] > 0 for row in plans),
        "maximum_actual_overflow": max((row["actual_overflow_after_execution"] for row in plans), default=0),
        "daily_active_security_age_sessions": distribution([row["holding_age_sessions"] for row in ages]),
        "realized_sell_age_sessions": distribution([row["holding_age_sessions"] for row in known]),
        "realized_sell_age_quantity_weighted_mean": float(sum((finite(row["quantity"])*row["holding_age_sessions"] for row in known), Decimal(0))/qsum) if qsum else None,
        "sell_fill_age_unresolved_count": len(sold)-len(known),
        "sell_fills_below_declared_minimum_age": sum(row["holding_age_sessions"] < minimum for row in known),
        "age_is_security_episode_not_per_lot": True,
        "pending_positive_exact_bonus_is_active_settled_fractional_claim_is_not": True}, sold


def terminal_invalid_exposure(folder, curve, initial, valid):
    """Describe stale physical exposure, without asserting a confirmed exchange.

    The optional zero-mark scenario changes the final recorded accounting value
    only. It does not replay orders, change an entitlement, or validate a path.
    """
    if valid:
        return {"status": "not_applicable_performance_valid_path"}
    if curve.empty:
        return {"status": "not_computed_no_valued_session", "performance_valid": False}
    day = curve.iloc[-1]["date"]
    audit = read_json(folder/"valuation_audit.json")
    terminal = [row for row in audit if row["session"] == day]
    if len(terminal) != 1:
        raise ValueError("Invalid path requires its unique last-session valuation audit")
    terminal = terminal[0]
    fields = ("unknown_price_instruments", "unpriced_delisted_claims", "unobserved_rights_ex_price_instruments")
    affected = set().union(*(set(terminal.get(key, [])) for key in fields))
    positions = read_csv(folder/"positions.csv")
    rows = positions.loc[positions.session.eq(day) & positions.instrument.isin(affected)] if not positions.empty else positions
    if not rows.empty and rows.instrument.duplicated().any():
        raise ValueError("Terminal physical exposure contains duplicate securities")
    exposures = [{"instrument": row["instrument"], "actual_raw_shares": row["quantity"],
        "recorded_mark_cny": row["price"], "recorded_mark_date": row["price_date"],
        "recorded_market_value_cny": row["market_value"], "valuation_source": row["valuation_source"],
        "invalidity_flags": [key for key in fields if row["instrument"] in terminal.get(key, [])]}
        for row in rows.to_dict("records")]
    deduction = total(rows, "market_value")
    recorded = finite(curve.iloc[-1]["equity_cny"])
    sensitivity = None
    if exposures:
        if deduction < 0 or deduction > recorded:
            raise ValueError("Final zero-mark sensitivity deduction is outside recorded NAV")
        sensitivity = {"kind": "final_recorded_physical_position_mark_zero_sensitivity_only",
            "method": "subtract terminal invalid physical positions' recorded market values once; no trade or entitlement changes",
            "recorded_indicative_terminal_equity_cny": str(recorded),
            "terminal_invalid_position_value_removed_cny": str(deduction),
            "sensitivity_terminal_equity_cny": str(recorded-deduction),
            "sensitivity_cumulative_change_from_initial_cash": str((recorded-deduction)/initial-1),
            "change_from_recorded_cumulative_return_percentage_points": str(-100*deduction/initial),
            "is_rerun_or_backtest": False, "performance_valid_after_sensitivity": False,
            "drawdown_annualization_and_rolling_not_computed": True,
            "zero_recovery_assumed_as_sensitivity_not_verified_source_fact": True}
    return {"status": "invalid_diagnostic_exposure_only", "last_valued_session": day,
        "performance_valid": False, "terminal_valuation_audit": terminal,
        "physical_positions": exposures, "invalid_instruments_without_physical_position": sorted(affected-set(rows.instrument if not rows.empty else [])),
        "unbounded_rights": terminal.get("unbounded_rights", []),
        "zero_mark_final_value_sensitivity": sensitivity,
        "successor_delivery_or_tradability_not_inferred_from_announced_ratio": True}


def figures(output, paths, curves):
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir())/"stopat30m-a6-matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
    from matplotlib.lines import Line2D
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
        "axes.spines.top": False, "axes.spines.right": False})
    colors = dict(zip(VARIANTS, ("#334155", "#2563eb", "#0d9488", "#d97706", "#9333ea")))
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
        gridspec_kw={"height_ratios": [2, 1]}, layout="constrained")
    fig.suptitle("Fixed LGB execution-only comparison | 2025 to 2026-10-19 (2026 partial)")
    handles = []
    for row in paths:
        if row["period"] != "continuous":
            continue
        variant = row["variant"]
        if row["performance_valid"]:
            dates, values, drawdown = curves[(variant, "continuous")]
            nav = np.asarray(values)
            axes[0].plot(pd.to_datetime(dates), nav, color=colors[variant], linewidth=1.7)
            axes[1].plot(pd.to_datetime(dates), drawdown, color=colors[variant], linewidth=1.3)
            handles.append(Line2D([], [], color=colors[variant], label=variant))
        else:
            handles.append(Line2D([], [], color=colors[variant], linestyle="none", marker="x", label=variant+" INVALID; diagnostic only"))
    axes[0].set_ylabel("Net NAV; 2024-12-31 cash anchor = 1")
    axes[0].ticklabel_format(axis="y", style="plain", useOffset=False)
    axes[0].legend(handles=handles, fontsize=9)
    axes[1].set_ylabel("Conservative drawdown: lower NAV / upper peak")
    axes[1].yaxis.set_major_formatter(PercentFormatter(1))
    for ax in axes:
        ax.grid(alpha=.18)
    fig.savefig(output/"continuous_nav_drawdown.png", dpi=180)
    plt.close(fig)
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), layout="constrained")
    fig.suptitle("All predeclared variants and separate cash periods | invalid paths excluded")
    specs = [("net_return", "Net return", 1), ("max_drawdown", "Maximum drawdown depth", -1),
        ("one_way_turnover_average_nav", "One-way turnover / mean NAV", 1)]
    x = np.arange(len(VARIANTS)); width = .25
    for ax, (metric, label, sign) in zip(axes, specs):
        for index, period in enumerate(PERIODS):
            values = []
            for variant in VARIANTS:
                row = next(item for item in paths if item["variant"] == variant and item["period"] == period)
                values.append(sign*row["verified_performance"][metric] if row["performance_valid"] else np.nan)
            ax.bar(x+(index-1)*width, values, width=width, label=period+(" partial" if period == "2026" else ""),
                color=("#94a3b8", "#60a5fa", "#0f766e")[index])
            for spot, value in zip(x+(index-1)*width, values):
                if np.isnan(value):
                    ax.text(spot, 0, "invalid", rotation=90, va="bottom", fontsize=7, color="#b91c1c")
        ax.set_xticks(x, VARIANTS, rotation=35, ha="right")
        ax.set_title(label); ax.axhline(0, color="#64748b", linewidth=.6); ax.grid(axis="y", alpha=.15)
        if metric != "one_way_turnover_average_nav":
            ax.yaxis.set_major_formatter(PercentFormatter(1))
    axes[0].legend(fontsize=8)
    fig.savefig(output/"net_return_drawdown_turnover.png", dpi=180)
    plt.close(fig)


def analyze(experiment_dir, output_dir):
    source, output = Path(experiment_dir).resolve(), Path(output_dir).resolve()
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("Use a new analysis directory separate from the immutable experiment")
    manifest, report = read_json(source/"manifest.json"), read_json(source/"report.json")
    if report.get("kind") != KIND or manifest.get("kind") != KIND:
        raise ValueError("A completed fixed A6 turnover comparison is required")
    verify_manifest(source, manifest)
    bound = {"analysis_script": proof(__file__), "source_report": proof(source/"report.json"),
        "source_manifest": proof(source/"manifest.json")}
    collect_proofs(report.get("bound_inputs"), bound, "source_original")
    collect_proofs(report.get("declaration"), bound, "source_declaration")
    if not report.get("bound_inputs"):
        raise ValueError("Source-bound results are required")
    verify_proofs(bound)
    declaration = read_json(source/"declaration.json")
    if proof(source/"declaration.json") != report["declaration"]:
        raise ValueError("Saved declaration differs from its frozen report proof")
    expected_terms = [dict(key=key, minimum_hold_sessions=hold, max_replacements=cap)
        for key, (hold, cap) in zip(VARIANTS, TERMS)]
    design = declaration["design"]
    if design["variants"] != expected_terms or design["periods"] != list(PERIODS) or design["evaluation_end"] != "2026-10-19":
        raise ValueError("Analysis requires all five predeclared variants and exact periods")
    protocol = read_json(report["bound_inputs"]["protocol"]["path"])
    if protocol["periods"] != {"2025": ["2025-01-01", "2025-12-31"],
            "2026": ["2026-01-01", "2026-10-19"], "continuous": ["2025-01-01", "2026-10-19"]}:
        raise ValueError("The unchanged separate2025/2026 accounting windows are required")
    config = protocol["execution"]
    initial = finite(config["initial_cash_cny"])
    snapshot_manifest_path = Path(report["bound_inputs"]["snapshot_manifest"]["path"])
    snapshot_manifest = read_json(snapshot_manifest_path)
    calendar_path = snapshot_manifest_path.parent/"dataset/calendars/day.txt"
    expected_calendar_sha = snapshot_manifest["files"]["calendars/day.txt"]["sha256"]
    bound["consumed_actual_calendar"] = {"path": str(calendar_path.resolve()), "sha256": expected_calendar_sha}
    verify_proofs(bound)
    calendar = calendar_path.read_text().splitlines()
    if calendar != sorted(set(calendar)):
        raise ValueError("Unique ordered actual source sessions are required")
    results = report["results"]
    pairs = [(row["variant"], row["period"]) for row in results]
    if len(pairs) != 15 or set(pairs) != {(variant, period) for variant in VARIANTS for period in PERIODS}:
        raise ValueError("Exactly all15 fixed paths are required")
    if report.get("all_performance_valid") is not all(row.get("performance_valid") is True for row in results):
        raise ValueError("Run-wide and path validity flags disagree")
    if set(report.get("baseline_full_reproduction", {})) != set(PERIODS) or any(
            row.get("passed") is not True for row in report["baseline_full_reproduction"].values()):
        raise ValueError("All three complete original baselines must have reproduced before treatments")
    paths, flat, monthly, rolling_rows, sell_rows, curves, qualities = [], [], [], [], [], {}, {}
    for variant in VARIANTS:
        for period in PERIODS:
            result = next(row for row in results if row["variant"] == variant and row["period"] == period)
            folder = source/variant/period
            if read_json(folder/"summary.json") != result:
                raise ValueError("Canonical summary differs from run report")
            if result.get("model") != "LGB" or result.get("variant_terms") != expected_terms[VARIANTS.index(variant)]:
                raise ValueError("Result differs from fixed LGB variant terms")
            curve, trades, orders, actions = [read_csv(folder/(name+".csv")) for name in
                ("curves", "trades", "orders", "corporate_actions")]
            quality = read_json(folder/"score_quality.json")
            if quality != result.get("score_quality") or period in qualities and qualities[period] != quality:
                raise ValueError("Fixed prediction quality must be unchanged across variants")
            qualities[period] = quality
            valid = result.get("performance_valid") is True
            start, end = protocol["periods"][period]
            prior = [day for day in calendar if day < start]
            if not prior:
                raise ValueError("The actual preceding session anchor is required")
            expected_days = [day for day in calendar if start <= day <= end]
            actual_days = curve.date.tolist() if not curve.empty else []
            if actual_days != expected_days[:len(actual_days)]:
                raise ValueError("A diagnostic path must be an ordered actual-session prefix")
            if valid and (actual_days != expected_days or result.get("stopped") is not None
                    or not curve.accounting_valid.eq("True").all()):
                raise ValueError("A valid path requires complete sessions and valid accounting")
            diagnostic, dates, nav = path_diagnostics(curve, initial, prior[-1])
            cost = cost_audit(result, trades, orders, actions, config)
            hold, sales = holding_audit(folder, result, trades, actions, calendar)
            terminal_exposure = terminal_invalid_exposure(folder, curve, initial, valid)
            for row in sales:
                sell_rows.append({"variant": variant, "period": period, **row})
            m = result["metrics"]
            if actual_days:
                close_enough(diagnostic["net_return"], m["total_return_lower_exact"], "net return")
                close_enough(diagnostic["max_drawdown"], m["max_drawdown_conservative_exact"], "drawdown")
                close_enough(diagnostic["mean_invested_fraction"], m["mean_invested_fraction"], "invested fraction")
                turnover = float(total(trades, "notional")/Decimal(str(curve.equity_cny.to_numpy(dtype=float).mean()))/2)
                close_enough(turnover, m["one_way_turnover_average_nav"], "turnover")
            performance = {"net_return": float(finite(m["total_return_lower_exact"])),
                "max_drawdown": float(finite(m["max_drawdown_conservative_exact"])),
                "one_way_turnover_average_nav": m["one_way_turnover_average_nav"],
                "mean_invested_fraction": m["mean_invested_fraction"], "diagnostics": diagnostic}
            paths.append({"variant": variant, "variant_terms": result["variant_terms"], "period": period,
                "performance_valid": valid, "classification": "verified_declared_accounting_performance" if valid else "invalid_diagnostic_only_do_not_select",
                "verified_performance": performance if valid else None,
                "diagnostic_only": None if valid else {"unverified_carried_ledger": performance, "stopped": result.get("stopped")},
                "recorded_original_metrics": m,
                "terminal_invalid_exposure": terminal_exposure,
                "cost_audit": cost, "holding_and_exit_audit": hold, "fixed_score_quality": quality})
            flat.append({"variant": variant, "period": period, "performance_valid": valid,
                **{key: value if valid else None for key, value in performance.items() if key != "diagnostics"},
                "indicative_diagnostic_net_return": None if valid else performance["net_return"],
                "fill_count": cost["executed_fill_count"], "fees_plus_slippage_cny": cost["fees_plus_slippage_cny"],
                "commission_cny": cost["commission_cny"], "minimum_commission_fill_fraction": cost["minimum_commission_fill_fraction"],
                "holding_audit_status": hold["status"], "realized_sell_age_mean": hold.get("realized_sell_age_sessions", {}).get("mean"),
                "sell_age_unknown_count": hold.get("sell_fill_age_unresolved_count"),
                "minimum_hold_locked_decisions": hold.get("minimum_hold_locked_security_decisions"),
                "cap_forced_retained_decisions": hold.get("cap_forced_retained_security_decisions"),
                "exit_intent_count": hold.get("exit_intent_count"), "actual_exit_fill_count": hold.get("actual_exit_fill_count"),
                "actual_overflow_rebalances": hold.get("actual_overflow_rebalances")})
            for month, value in diagnostic.get("monthly_returns", {}).items():
                monthly.append({"variant": variant, "period": period, "month": month,
                    "performance_valid": valid, "verified_net_return": value if valid else None,
                    "indicative_diagnostic_return": None if valid else value})
            for length, data in diagnostic.get("rolling_actual_sessions", {}).items():
                rolling_rows.extend({"variant": variant, "period": period, "window_actual_sessions": int(length),
                    "date": row["date"], "performance_valid": valid,
                    "verified_net_return": row["net_return"] if valid else None,
                    "indicative_diagnostic_return": None if valid else row["net_return"]} for row in data["daily"])
            curves[(variant, period)] = dates, nav, diagnostic.get("conservative_drawdown_daily", [0.])
    verify_manifest(source, manifest); verify_proofs(bound)
    output.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(flat).to_csv(output/"comparison.csv", index=False)
    pd.DataFrame(monthly).to_csv(output/"monthly_returns.csv", index=False)
    pd.DataFrame(rolling_rows).to_csv(output/"rolling_returns.csv", index=False)
    pd.DataFrame(sell_rows).to_csv(output/"realized_sell_holding_age.csv", index=False)
    figures(output, paths, curves)
    analysis = {"kind": "a6_fixed_lgb_turnover_read_only_analysis", "created_at": datetime.now(timezone.utc).isoformat(),
        "all15paths_verified": True, "all_performance_valid": report["all_performance_valid"],
        "periods": protocol["periods"], "variants": expected_terms, "paths": paths,
        "source_decision_unmodified": report.get("decision"), "bound_inputs": bound,
        "limitations": ["Previously examined historical windows; not fresh future out-of-sample evidence.",
            "2026 ends2026-10-19 and is a partial-year result; annual accounts start with separate cash.",
            "Net accounting NAV is not liquidation value or proof of real broker fills.",
            "Costs are recorded attribution, not an additive no-cost return counterfactual.",
            "Security-episode holding age is not per-lot age; baseline extra audits remain uncomputed.",
            "Invalid paths are diagnostic only and excluded from verified return columns and charts.",
            "No models, strategies, thresholds, active paper accounts or broker orders are changed."]}
    save(output/"analysis.json", analysis)
    verify_manifest(source, manifest); verify_proofs(bound)
    save(output/"manifest.json", {"kind": analysis["kind"], "status": "read_only_analysis_complete",
        "bound_inputs": bound, "all15paths_verified": True, "files": {
            path.name: sha(path) for path in sorted(output.iterdir()) if path.is_file()}})
    return analysis


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    result = analyze(args.experiment_dir, args.output_dir)
    print(json.dumps({"output": str(args.output_dir.resolve()), "all15paths_verified": True,
        "all_performance_valid": result["all_performance_valid"], "new_strategy_selected": False}))


if __name__ == "__main__":
    main()
